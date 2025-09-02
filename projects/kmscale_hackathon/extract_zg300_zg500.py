#!/bin/python
# -*- coding: utf-8 -*-

"""
This script extracts the zg300 and zg500 data from the given file.
It reads the data from a file, processes it, and writes the extracted data to a new file.
"""
import os
import sys
import xarray as xr
import glob

def extract_zg300_zg500(input_file, output_file):
    """
    Extracts zg300 and zg500 data from the input file and writes it to the output file.
    Automatically detects if pressure levels are in Pa or hPa.

    Parameters:
    input_file (str): The path to the input file.
    output_file (str): The path to the output file.
    """
    # Open the input file
    with xr.open_dataset(input_file) as ds:
        # Determine pressure level units based on maximum value
        # If max level < 1500, assume hPa, otherwise Pa
        lev_values = ds['lev'].values
        
        if lev_values.max() < 1500:
            # Levels are in hPa
            print(f"Detected hPa levels in {input_file}")
            level_300, level_500 = 300, 500
        else:
            # Levels are in Pa
            print(f"Detected Pa levels in {input_file}")
            level_300, level_500 = 30000, 50000
            
        # Extract zg300 and zg500 data
        zg300 = ds['zg'].sel(lev=level_300, method='nearest').drop_vars('lev')
        zg500 = ds['zg'].sel(lev=level_500, method='nearest').drop_vars('lev')
        
        # Log which levels were actually selected
        print(f"Selected level for 300 hPa: {ds['lev'].sel(lev=level_300, method='nearest').values}")
        print(f"Selected level for 500 hPa: {ds['lev'].sel(lev=level_500, method='nearest').values}")
        
        # Check if the data is empty
        if zg300.size == 0 or zg500.size == 0:
            print(f"Warning: No data found for zg300 or zg500 in {input_file}.")
            return

        # Create a new dataset with the extracted data
        new_ds = xr.Dataset({'zg300': zg300, 'zg500': zg500})

        # Write the new dataset to the output file
        new_ds.to_netcdf(output_file)

def main():
    """
    Main function to handle command line arguments and call the extract function.
    """
    # Check if we have at least 3 arguments (script name, at least one input file, output path)
    if len(sys.argv) < 3:
        print("Usage: python extract_zg300_zg500.py <input_files> <output_path>")
        sys.exit(1)

    # Last argument is the output path
    output_path = sys.argv[-1]
    
    # All arguments except the script name and output path are input files
    input_files = ','.join(sys.argv[1:-1])

    # Import glob module for wildcard file matching
    
    # Process each pattern (comma-separated)
    all_files = []
    for pattern in input_files.split(','):
        matching_files = glob.glob(pattern)
        if not matching_files:
            print(f"Warning: No files match pattern '{pattern}'")
            continue
        all_files.extend(matching_files)
    
    # Check if any files were found
    if not all_files:
        print("Error: No valid input files found.")
        sys.exit(1)
        
    for input_file in all_files:
        # Check if the input file is a valid file
        if not os.path.isfile(input_file):
            print(f"Error: Input file {input_file} does not exist.")
            continue

        # Create the output file name based on the input file name
        output_file = os.path.join(output_path, 
                                   os.path.basename(input_file).replace('.nc', '_zg300_zg500.nc'))

        # Extract zg300 and zg500 data
        extract_zg300_zg500(input_file, output_file)


if __name__ == "__main__":
    # Call the main function
    main()
# The script is designed to be run from the command line with two arguments:
# 1. The input files (can be a single file or a comma-separated list of files).
# 2. The output path where the extracted data will be saved.