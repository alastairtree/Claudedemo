#!/usr/bin/env python
"""Explore CDF files to understand their structure."""

import cdflib
from pathlib import Path

# Open the smaller CDF file first
cdf_path = Path("tests/data/solo_L2_mag-rtn-normal-1-minute-internal_20241225_V00.cdf")

print(f"Opening CDF file: {cdf_path.name}")
print("=" * 80)

# Open the CDF file
cdf = cdflib.CDF(str(cdf_path))

# Get basic CDF information
print("\n1. CDF INFO:")
print("-" * 80)
info = cdf.cdf_info()
print(f"CDF Version: {info.Version}")
print(f"Encoding: {info.Encoding}")
print(f"Majority: {info.Majority}")
print(f"Number of rVariables: {len(info.rVariables)}")
print(f"Number of zVariables: {len(info.zVariables)}")

# List all variables
print("\n2. VARIABLES:")
print("-" * 80)
all_vars = info.rVariables + info.zVariables
print(f"Total variables: {len(all_vars)}")
for var_name in all_vars:
    print(f"  - {var_name}")

# Get global attributes
print("\n3. GLOBAL ATTRIBUTES:")
print("-" * 80)
global_attrs = cdf.globalattsget()
for attr_name, attr_values in global_attrs.items():
    print(f"  {attr_name}: {attr_values}")

# Explore first few variables in detail
print("\n4. VARIABLE DETAILS (first 5 variables):")
print("-" * 80)
for var_name in all_vars[:5]:
    print(f"\n  Variable: {var_name}")

    # Get variable attributes
    var_attrs = cdf.varattsget(var_name)
    if var_attrs:
        print(f"    Attributes:")
        for attr_name, attr_value in var_attrs.items():
            print(f"      {attr_name}: {attr_value}")

    # Get variable data
    try:
        data = cdf.varget(var_name)
        print(f"    Data shape: {data.shape if hasattr(data, 'shape') else 'scalar'}")
        print(f"    Data type: {type(data).__name__}")
        print(f"    Number of records: {len(data) if hasattr(data, '__len__') else 1}")

        # Show first few records
        if hasattr(data, '__len__') and len(data) > 0:
            print(f"    First record: {data[0]}")
            if len(data) > 1:
                print(f"    Second record: {data[1]}")
    except Exception as e:
        print(f"    Error reading data: {e}")

# Try the larger file too
print("\n\n" + "=" * 80)
cdf_path2 = Path("tests/data/imap_mag_l1c_norm-magi_20251010_v001.cdf")
print(f"\nOpening second CDF file: {cdf_path2.name}")
print("=" * 80)

cdf2 = cdflib.CDF(str(cdf_path2))
info2 = cdf2.cdf_info()
print(f"Number of variables: {len(info2.rVariables) + len(info2.zVariables)}")
all_vars2 = info2.rVariables + info2.zVariables
print("Variables:")
for var_name in all_vars2:
    print(f"  - {var_name}")

print("\n" + "=" * 80)
print("Exploration complete!")
