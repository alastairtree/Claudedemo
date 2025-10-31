"""CDF to CSV extraction functionality."""

from __future__ import annotations

import csv
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from data_sync.cdf_reader import CDFVariable, get_column_names_for_variable, read_cdf_variables
from data_sync.config import SyncJob


@dataclass
class ExtractionResult:
    """Result of extracting data from a CDF file."""

    output_file: Path
    num_rows: int
    num_columns: int
    column_names: list[str]
    file_size: int
    variable_names: list[str]


def _make_unique_column_names(column_names: list[str]) -> list[str]:
    """Ensure all column names are unique by adding suffixes if needed.

    Args:
        column_names: List of column names that may contain duplicates

    Returns:
        List of unique column names
    """
    seen: dict[str, int] = {}
    unique_names = []

    for name in column_names:
        if name not in seen:
            seen[name] = 0
            unique_names.append(name)
        else:
            seen[name] += 1
            unique_names.append(f"{name}_{seen[name]}")

    return unique_names


def _group_variables_by_record_count(
    variables: list[CDFVariable],
) -> dict[int, list[CDFVariable]]:
    """Group variables by their record count.

    Args:
        variables: List of CDFVariable objects

    Returns:
        Dictionary mapping record count to list of variables
    """
    groups: dict[int, list[CDFVariable]] = {}

    for var in variables:
        if var.num_records not in groups:
            groups[var.num_records] = []
        groups[var.num_records].append(var)

    return groups


def _expand_variable_to_columns(
    variable: CDFVariable, cdf_file_path: Path, max_records: int | None = None
) -> tuple[list[str], list[list[Any]], int]:
    """Expand a CDF variable into column names and data columns.

    Args:
        variable: The variable to expand
        cdf_file_path: Path to the CDF file
        max_records: Maximum number of records to extract (None = all)

    Returns:
        Tuple of (column_names, data_columns, actual_records) where data_columns is a list of columns
        and actual_records is the number of records actually extracted
    """
    column_names = get_column_names_for_variable(variable, cdf_file_path)

    # Determine how many records to extract
    actual_records = variable.num_records
    if max_records is not None:
        actual_records = min(actual_records, max_records)

    if not variable.is_array:
        # 1D variable - single column
        if isinstance(variable.data, np.ndarray):
            data = variable.data[:actual_records].tolist()
        else:
            data = [variable.data]
        data_columns = [data]
    else:
        # 2D variable - multiple columns
        data_columns = []
        for i in range(variable.array_size):
            column_data = variable.data[:actual_records, i].tolist()
            data_columns.append(column_data)

    return column_names, data_columns, actual_records


def _generate_output_filename(
    template: str, source_file: Path, variable_name: str | None = None
) -> str:
    """Generate output filename from template.

    Args:
        template: Filename template with [SOURCE_FILE] and [VARIABLE_NAME] placeholders
        source_file: Source CDF file path
        variable_name: Variable name (optional)

    Returns:
        Generated filename
    """
    filename = template
    filename = filename.replace("[SOURCE_FILE]", source_file.stem)

    if variable_name:
        filename = filename.replace("[VARIABLE_NAME]", variable_name)
    else:
        # If no variable name, remove the placeholder
        filename = filename.replace("-[VARIABLE_NAME]", "").replace("_[VARIABLE_NAME]", "")
        filename = filename.replace("[VARIABLE_NAME]-", "").replace("[VARIABLE_NAME]_", "")
        filename = filename.replace("[VARIABLE_NAME]", "")

    return filename


def _get_unique_filename(base_filename: str, used_filenames: set[str]) -> str:
    """Get a unique filename by adding a numerical suffix if needed.

    Args:
        base_filename: The base filename to use
        used_filenames: Set of filenames already used in this extraction

    Returns:
        A unique filename
    """
    # If filename hasn't been used yet in this extraction, use it as-is
    if base_filename not in used_filenames:
        return base_filename

    # Add numerical suffix to make it unique
    base_name = Path(base_filename).stem
    extension = Path(base_filename).suffix
    counter = 1

    while True:
        new_filename = f"{base_name}_{counter}{extension}"
        if new_filename not in used_filenames:
            return new_filename
        counter += 1


def _validate_existing_csv_header(csv_path: Path, expected_columns: list[str]) -> bool:
    """Validate that an existing CSV has the expected header.

    Args:
        csv_path: Path to the CSV file
        expected_columns: Expected column names

    Returns:
        True if headers match, False otherwise
    """
    try:
        with open(csv_path, encoding="utf-8") as f:
            reader = csv.reader(f)
            existing_header = next(reader, None)
            return existing_header == expected_columns
    except Exception:
        return False


def extract_cdf_to_csv(
    cdf_file_path: Path,
    output_dir: Path,
    filename_template: str = "[SOURCE_FILE]-[VARIABLE_NAME].csv",
    automerge: bool = True,
    append: bool = False,
    variable_names: list[str] | None = None,
    max_records: int | None = None,
) -> list[ExtractionResult]:
    """Extract data from a CDF file to CSV files.

    Args:
        cdf_file_path: Path to the CDF file
        output_dir: Directory to save CSV files
        filename_template: Template for output filenames
        automerge: Whether to merge variables with same record count
        append: Whether to append to existing files
        variable_names: List of specific variables to extract (None = all)
        max_records: Maximum number of records to extract per variable (None = all)

    Returns:
        List of ExtractionResult objects

    Raises:
        ValueError: If specified variables are not found
        FileExistsError: If output file exists and append is False
        ValueError: If appending but headers don't match
    """
    # Read all variables
    all_variables = read_cdf_variables(cdf_file_path)

    # Filter variables if specific ones are requested
    if variable_names:
        filtered_vars = []
        requested_set = set(variable_names)
        found_set = set()

        for var in all_variables:
            if var.name in requested_set:
                filtered_vars.append(var)
                found_set.add(var.name)

        # Check for missing variables
        missing = requested_set - found_set
        if missing:
            raise ValueError(f"Variables not found in CDF file: {', '.join(sorted(missing))}")

        variables = filtered_vars
    else:
        variables = all_variables

    if not variables:
        return []

    # Ensure output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    results = []
    used_filenames: set[str] = set()  # Track filenames used in this extraction

    if automerge:
        # Group variables by record count and create merged CSV files
        groups = _group_variables_by_record_count(variables)

        for record_count, group_vars in sorted(groups.items(), key=lambda x: -x[0]):
            # Skip variables with very few records (likely metadata)
            if record_count < 2:
                continue

            # Collect all columns from this group
            all_column_names = []
            all_data_columns = []
            var_names_in_group = []
            actual_records_list = []

            for var in group_vars:
                col_names, data_cols, actual_records = _expand_variable_to_columns(
                    var, cdf_file_path, max_records
                )
                all_column_names.extend(col_names)
                all_data_columns.extend(data_cols)
                var_names_in_group.append(var.name)
                actual_records_list.append(actual_records)

            # Use the minimum actual records across all variables in the group
            actual_record_count = min(actual_records_list) if actual_records_list else 0

            # Make column names unique
            all_column_names = _make_unique_column_names(all_column_names)

            # Generate filename using first variable name
            primary_var = var_names_in_group[0]
            base_filename = _generate_output_filename(filename_template, cdf_file_path, primary_var)

            # Get unique filename (add numerical suffix if needed)
            output_filename = _get_unique_filename(base_filename, used_filenames)
            used_filenames.add(output_filename)
            output_path = output_dir / output_filename

            # Check for existing file
            if output_path.exists() and not append:
                raise FileExistsError(
                    f"Output file already exists: {output_path}. "
                    "Use --append to add data to existing file."
                )

            # Validate header if appending
            if (
                append
                and output_path.exists()
                and not _validate_existing_csv_header(output_path, all_column_names)
            ):
                raise ValueError(
                    f"Cannot append to {output_path}: "
                    f"existing CSV has different columns. "
                    f"Expected columns: {', '.join(all_column_names)}"
                )

            # Write CSV
            mode = "a" if append and output_path.exists() else "w"
            write_header = mode == "w"

            with open(output_path, mode, newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                if write_header:
                    writer.writerow(all_column_names)

                # Transpose data to write rows
                for row_idx in range(actual_record_count):
                    row = [col[row_idx] for col in all_data_columns]
                    writer.writerow(row)

            file_size = output_path.stat().st_size
            results.append(
                ExtractionResult(
                    output_file=output_path,
                    num_rows=actual_record_count,
                    num_columns=len(all_column_names),
                    column_names=all_column_names,
                    file_size=file_size,
                    variable_names=var_names_in_group,
                )
            )

    else:
        # Create separate CSV for each variable
        for var in variables:
            # Skip variables with very few records
            if var.num_records < 2:
                continue

            col_names, data_cols, actual_records = _expand_variable_to_columns(
                var, cdf_file_path, max_records
            )
            col_names = _make_unique_column_names(col_names)

            base_filename = _generate_output_filename(filename_template, cdf_file_path, var.name)

            # Get unique filename (add numerical suffix if needed)
            output_filename = _get_unique_filename(base_filename, used_filenames)
            used_filenames.add(output_filename)
            output_path = output_dir / output_filename

            # Check for existing file
            if output_path.exists() and not append:
                raise FileExistsError(
                    f"Output file already exists: {output_path}. "
                    "Use --append to add data to existing file."
                )

            # Validate header if appending
            if (
                append
                and output_path.exists()
                and not _validate_existing_csv_header(output_path, col_names)
            ):
                raise ValueError(
                    f"Cannot append to {output_path}: "
                    f"existing CSV has different columns. "
                    f"Expected columns: {', '.join(col_names)}"
                )

            # Write CSV
            mode = "a" if append and output_path.exists() else "w"
            write_header = mode == "w"

            with open(output_path, mode, newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                if write_header:
                    writer.writerow(col_names)

                # Transpose data to write rows
                for row_idx in range(actual_records):
                    row = [col[row_idx] for col in data_cols]
                    writer.writerow(row)

            file_size = output_path.stat().st_size
            results.append(
                ExtractionResult(
                    output_file=output_path,
                    num_rows=actual_records,
                    num_columns=len(col_names),
                    column_names=col_names,
                    file_size=file_size,
                    variable_names=[var.name],
                )
            )

    return results


def extract_cdf_with_config(
    cdf_file_path: Path,
    output_path: Path,
    job: SyncJob,
    max_records: int | None = None,
) -> ExtractionResult:
    """Extract data from a CDF file to CSV using job configuration for column selection and mapping.

    This function extracts CDF data and applies the same column mappings and transformations
    that would be used when syncing to a database, but outputs to CSV instead.

    Args:
        cdf_file_path: Path to the CDF file
        output_path: Path to the output CSV file
        job: SyncJob configuration with column mappings and transformations
        max_records: Maximum number of records to extract (None = all)

    Returns:
        ExtractionResult with details about the extracted CSV

    Raises:
        ValueError: If CDF extraction fails or column mappings are invalid
        FileNotFoundError: If CDF file doesn't exist
    """
    # Step 1: Extract CDF to temporary CSV (raw dump)
    temp_dir = Path(tempfile.mkdtemp(prefix="data_sync_extract_"))
    try:
        raw_results = extract_cdf_to_csv(
            cdf_file_path=cdf_file_path,
            output_dir=temp_dir,
            filename_template=f"{cdf_file_path.stem}_raw.csv",
            automerge=True,
            append=False,
            variable_names=None,
            max_records=max_records,
        )

        if not raw_results:
            raise ValueError("No data could be extracted from CDF file")

        # Use the first (and likely only) extracted CSV
        raw_csv_path = raw_results[0].output_file

        # Step 2: Extract values from filename if configured
        filename_values = None
        if job.filename_to_column:
            filename_values = job.filename_to_column.extract_values_from_filename(cdf_file_path)

        # Step 3: Read raw CSV and apply transformations
        with open(raw_csv_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                raise ValueError("Extracted CSV has no columns")

            csv_columns = set(reader.fieldnames)

            # Step 4: Determine which columns to include in output
            # Include ID columns
            output_columns = []
            sync_columns = []

            # Add ID columns
            for id_col in job.id_mapping:
                if id_col.expression or id_col.function:
                    # Custom function for ID
                    sync_columns.append(id_col)
                    output_columns.append(id_col.db_column)
                elif id_col.csv_column and id_col.csv_column in csv_columns:
                    sync_columns.append(id_col)
                    output_columns.append(id_col.db_column)
                else:
                    raise ValueError(
                        f"ID column '{id_col.csv_column}' not found in CDF data. "
                        f"Available columns: {', '.join(sorted(csv_columns))}"
                    )

            # Add data columns
            if job.columns:
                for col in job.columns:
                    if col.expression or col.function:
                        # Custom function - always include
                        sync_columns.append(col)
                        output_columns.append(col.db_column)
                    elif col.csv_column and col.csv_column in csv_columns:
                        sync_columns.append(col)
                        output_columns.append(col.db_column)
                    elif col.csv_column:
                        # Column specified but not found - this might be OK if it's optional
                        # For now, we'll skip it with a warning
                        pass
            else:
                # No columns specified - include all columns from CSV
                for csv_col in sorted(csv_columns):
                    # Check if this column is already in id_mapping
                    if not any(
                        id_col.csv_column == csv_col for id_col in job.id_mapping if id_col.csv_column
                    ):
                        # Import ColumnMapping here to avoid circular import
                        from data_sync.config import ColumnMapping

                        col_mapping = ColumnMapping(csv_column=csv_col, db_column=csv_col)
                        sync_columns.append(col_mapping)
                        output_columns.append(csv_col)

            # Add filename-extracted columns if configured
            if filename_values:
                for col_name, col_mapping in job.filename_to_column.columns.items():
                    if col_name in filename_values:
                        output_columns.append(col_mapping.db_column)

            # Step 5: Process rows and write output CSV
            output_path.parent.mkdir(parents=True, exist_ok=True)
            rows_written = 0

            # Reset reader to beginning
            f.seek(0)
            reader = csv.DictReader(f)

            with open(output_path, "w", newline="", encoding="utf-8") as out_f:
                writer = csv.DictWriter(out_f, fieldnames=output_columns)
                writer.writeheader()

                for row in reader:
                    output_row = {}

                    # Process each column mapping
                    for col_mapping in sync_columns:
                        # Check if this column uses a custom function/expression
                        if col_mapping.expression or col_mapping.function:
                            # Apply custom function/expression
                            output_row[col_mapping.db_column] = col_mapping.apply_custom_function(row)
                        elif col_mapping.csv_column and col_mapping.csv_column in row:
                            csv_value = row[col_mapping.csv_column]
                            # Apply lookup transformation if configured
                            output_row[col_mapping.db_column] = col_mapping.apply_lookup(csv_value)

                    # Add filename values if configured
                    if filename_values:
                        for col_name, col_mapping in job.filename_to_column.columns.items():
                            if col_name in filename_values:
                                output_row[col_mapping.db_column] = filename_values[col_name]

                    writer.writerow(output_row)
                    rows_written += 1

        file_size = output_path.stat().st_size

        return ExtractionResult(
            output_file=output_path,
            num_rows=rows_written,
            num_columns=len(output_columns),
            column_names=output_columns,
            file_size=file_size,
            variable_names=raw_results[0].variable_names,
        )

    finally:
        # Clean up temporary files
        try:
            if raw_csv_path.exists():
                raw_csv_path.unlink()
            if temp_dir.exists():
                temp_dir.rmdir()
        except Exception:
            pass  # Best effort cleanup
