"""CDF to CSV extraction functionality."""

from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np

from data_sync.cdf_reader import CDFVariable, get_column_names_for_variable, read_cdf_variables


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
    variable: CDFVariable, cdf_file_path: Path
) -> tuple[list[str], list[list[Any]]]:
    """Expand a CDF variable into column names and data columns.

    Args:
        variable: The variable to expand
        cdf_file_path: Path to the CDF file

    Returns:
        Tuple of (column_names, data_columns) where data_columns is a list of columns
    """
    column_names = get_column_names_for_variable(variable, cdf_file_path)

    if not variable.is_array:
        # 1D variable - single column
        data_columns = [
            variable.data.tolist() if isinstance(variable.data, np.ndarray) else [variable.data]
        ]
    else:
        # 2D variable - multiple columns
        data_columns = []
        for i in range(variable.array_size):
            column_data = variable.data[:, i].tolist()
            data_columns.append(column_data)

    return column_names, data_columns


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


def extract_cdf_to_csv(
    cdf_file_path: Path,
    output_dir: Path,
    filename_template: str = "[SOURCE_FILE]-[VARIABLE_NAME].csv",
    automerge: bool = True,
    append: bool = False,
    variable_names: list[str] | None = None,
) -> list[ExtractionResult]:
    """Extract data from a CDF file to CSV files.

    Args:
        cdf_file_path: Path to the CDF file
        output_dir: Directory to save CSV files
        filename_template: Template for output filenames
        automerge: Whether to merge variables with same record count
        append: Whether to append to existing files
        variable_names: List of specific variables to extract (None = all)

    Returns:
        List of ExtractionResult objects

    Raises:
        ValueError: If specified variables are not found
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

            for var in group_vars:
                col_names, data_cols = _expand_variable_to_columns(var, cdf_file_path)
                all_column_names.extend(col_names)
                all_data_columns.extend(data_cols)
                var_names_in_group.append(var.name)

            # Make column names unique
            all_column_names = _make_unique_column_names(all_column_names)

            # Generate filename (use first variable name or add record count for uniqueness)
            if len(var_names_in_group) == 1:
                primary_var = var_names_in_group[0]
            else:
                # For merged groups, use descriptive name with record count
                primary_var = f"merged_{record_count}records"

            output_filename = _generate_output_filename(
                filename_template, cdf_file_path, primary_var
            )
            output_path = output_dir / output_filename

            # Write CSV
            mode = "a" if append and output_path.exists() else "w"
            write_header = mode == "w" or not output_path.exists()

            with open(output_path, mode, newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                if write_header:
                    writer.writerow(all_column_names)

                # Transpose data to write rows
                for row_idx in range(record_count):
                    row = [col[row_idx] for col in all_data_columns]
                    writer.writerow(row)

            file_size = output_path.stat().st_size
            results.append(
                ExtractionResult(
                    output_file=output_path,
                    num_rows=record_count,
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

            col_names, data_cols = _expand_variable_to_columns(var, cdf_file_path)
            col_names = _make_unique_column_names(col_names)

            output_filename = _generate_output_filename(filename_template, cdf_file_path, var.name)
            output_path = output_dir / output_filename

            # Write CSV
            mode = "a" if append and output_path.exists() else "w"
            write_header = mode == "w" or not output_path.exists()

            with open(output_path, mode, newline="", encoding="utf-8") as f:
                writer = csv.writer(f)

                if write_header:
                    writer.writerow(col_names)

                # Transpose data to write rows
                for row_idx in range(var.num_records):
                    row = [col[row_idx] for col in data_cols]
                    writer.writerow(row)

            file_size = output_path.stat().st_size
            results.append(
                ExtractionResult(
                    output_file=output_path,
                    num_rows=var.num_records,
                    num_columns=len(col_names),
                    column_names=col_names,
                    file_size=file_size,
                    variable_names=[var.name],
                )
            )

    return results
