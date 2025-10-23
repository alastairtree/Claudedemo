#!/usr/bin/env python
"""Demonstration of extracting data from CDF files."""

import cdflib
import numpy as np
from pathlib import Path


def explore_cdf_variables(cdf_path: Path) -> None:
    """
    Iterate through all variables in a CDF file and extract data.

    Args:
        cdf_path: Path to the CDF file
    """
    print(f"\n{'=' * 80}")
    print(f"Exploring: {cdf_path.name}")
    print(f"{'=' * 80}\n")

    # Open the CDF file
    with cdflib.CDF(str(cdf_path)) as cdf:
        # Get CDF info
        info = cdf.cdf_info()
        all_vars = info.rVariables + info.zVariables

        print(f"Total variables: {len(all_vars)}\n")

        # Iterate through each variable
        for i, var_name in enumerate(all_vars, 1):
            print(f"{i}. Variable: {var_name}")
            print(f"   {'-' * 70}")

            # Get variable attributes
            var_attrs = cdf.varattsget(var_name)

            # Show key attributes
            if "FIELDNAM" in var_attrs:
                print(f"   Field Name: {var_attrs['FIELDNAM']}")
            if "CATDESC" in var_attrs:
                print(f"   Description: {var_attrs['CATDESC']}")
            if "UNITS" in var_attrs:
                print(f"   Units: {var_attrs['UNITS']}")
            if "VAR_TYPE" in var_attrs:
                print(f"   Type: {var_attrs['VAR_TYPE']}")

            # Get and analyze the data
            try:
                data = cdf.varget(var_name)

                # Determine data structure
                if isinstance(data, np.ndarray):
                    print(f"   Data shape: {data.shape}")
                    print(f"   Data type: {data.dtype}")
                    num_records = data.shape[0] if len(data.shape) > 0 else 1

                    print(f"   Number of records: {num_records}")

                    # Show record structure
                    if len(data.shape) == 1:
                        # 1D array - each element is a record
                        print(f"   Record structure: Single value per record")
                        print(f"   Sample records:")
                        for j in range(min(3, num_records)):
                            print(f"      Record {j}: {data[j]}")

                    elif len(data.shape) == 2:
                        # 2D array - each row is a record with multiple values
                        print(f"   Record structure: {data.shape[1]} values per record")
                        print(f"   Sample records:")
                        for j in range(min(3, num_records)):
                            print(f"      Record {j}: {data[j]}")

                    elif len(data.shape) > 2:
                        # Multi-dimensional data
                        print(f"   Record structure: Multi-dimensional ({data.shape[1:]} per record)")
                        print(f"   Sample records:")
                        for j in range(min(2, num_records)):
                            print(f"      Record {j} shape: {data[j].shape}")
                            print(f"      Record {j} sample: {data[j].flatten()[:5]}...")

                    # Show value range for numeric data
                    if np.issubdtype(data.dtype, np.number):
                        if not np.all(np.isnan(data)):
                            valid_data = data[~np.isnan(data)]
                            if len(valid_data) > 0:
                                print(f"   Value range: [{np.min(valid_data):.4f}, {np.max(valid_data):.4f}]")
                else:
                    # Non-numpy data
                    print(f"   Data type: {type(data).__name__}")
                    print(f"   Value: {data}")

            except Exception as e:
                print(f"   ERROR reading data: {e}")

            print()  # Blank line between variables


def extract_time_series_data(cdf_path: Path, time_var: str, data_var: str, max_records: int = 10) -> None:
    """
    Extract time-series data from a CDF file.

    Args:
        cdf_path: Path to the CDF file
        time_var: Name of the time variable
        data_var: Name of the data variable
        max_records: Maximum number of records to show
    """
    print(f"\n{'=' * 80}")
    print(f"Extracting Time Series: {data_var} from {cdf_path.name}")
    print(f"{'=' * 80}\n")

    with cdflib.CDF(str(cdf_path)) as cdf:
        # Get time data
        time_data = cdf.varget(time_var)
        print(f"Time variable: {time_var}")
        print(f"  Records: {len(time_data)}")
        print(f"  First timestamp: {time_data[0]}")
        print(f"  Last timestamp: {time_data[-1]}")

        # Get data variable
        data = cdf.varget(data_var)
        print(f"\nData variable: {data_var}")
        print(f"  Shape: {data.shape}")
        print(f"  Records: {len(data)}")

        # Show sample time-series records
        print(f"\nSample time-series data (first {min(max_records, len(data))} records):")
        print(f"{'Index':<8} {'Time':<25} {'Data':<50}")
        print(f"{'-' * 8} {'-' * 25} {'-' * 50}")

        for i in range(min(max_records, len(data))):
            time_val = time_data[i]
            data_val = data[i]

            # Format data based on shape
            if isinstance(data_val, np.ndarray):
                if len(data_val) <= 3:
                    data_str = f"[{', '.join(f'{v:.4f}' for v in data_val)}]"
                else:
                    data_str = f"[{', '.join(f'{v:.4f}' for v in data_val[:3])}...] ({len(data_val)} values)"
            else:
                data_str = f"{data_val}"

            print(f"{i:<8} {time_val:<25} {data_str:<50}")


# Main demonstration
if __name__ == "__main__":
    # Explore both CDF files
    cdf_files = [
        Path("tests/data/solo_L2_mag-rtn-normal-1-minute-internal_20241225_V00.cdf"),
        Path("tests/data/imap_mag_l1c_norm-magi_20251010_v001.cdf"),
    ]

    for cdf_file in cdf_files:
        if cdf_file.exists():
            explore_cdf_variables(cdf_file)

    # Demonstrate time-series extraction
    print(f"\n\n{'#' * 80}")
    print("TIME-SERIES EXTRACTION EXAMPLES")
    print(f"{'#' * 80}")

    # Example 1: Solar Orbiter MAG data
    solo_path = Path("tests/data/solo_L2_mag-rtn-normal-1-minute-internal_20241225_V00.cdf")
    if solo_path.exists():
        extract_time_series_data(solo_path, "EPOCH", "B_RTN", max_records=10)

    # Example 2: IMAP MAG data
    imap_path = Path("tests/data/imap_mag_l1c_norm-magi_20251010_v001.cdf")
    if imap_path.exists():
        extract_time_series_data(imap_path, "epoch", "vectors", max_records=10)

    print(f"\n{'=' * 80}")
    print("DEMONSTRATION COMPLETE!")
    print(f"{'=' * 80}\n")
