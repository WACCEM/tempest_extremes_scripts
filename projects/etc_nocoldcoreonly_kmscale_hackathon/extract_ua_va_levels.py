#!/bin/python
# -*- coding: utf-8 -*-

"""
This script extracts wind data (ua and va) at specified pressure levels from 3D fields.
It reads the data from a file, processes it, and writes the extracted data to a new file.
"""
import os
import argparse
import xarray as xr
import glob

def extract_ua_va_levels(input_file, output_file, levels):
    """
    Extracts ua and va data at specified pressure levels from the input file and writes it to the output file.
    Automatically detects if pressure levels are in Pa or hPa.

    Parameters:
    input_file (str): The path to the input file.
    output_file (str): The path to the output file.
    levels (list): List of pressure levels in hPa (e.g., [1000, 925, 850, 700, 500, 300, 250]).
    """
    # Open the input file
    with xr.open_dataset(input_file) as ds:
        # Determine pressure level units based on maximum value
        # If max level < 1500, assume hPa, otherwise Pa
        lev_values = ds['lev'].values if 'lev' in ds else ds['pressure'].values
        coord_name = 'lev' if 'lev' in ds else 'pressure'
        
        if lev_values.max() < 1500:
            # Levels are in hPa
            print(f"Detected hPa levels in {input_file}")
            scale_factor = 1
        else:
            # Levels are in Pa
            print(f"Detected Pa levels in {input_file}")
            scale_factor = 100
            
        # Create a new dataset to store extracted data
        data_vars = {}
        
        # Extract ua and va data for each level
        for level_hpa in levels:
            level_target = level_hpa * scale_factor
            
            try:
                # Extract ua and va data at this level
                ua_level = ds['ua'].sel({coord_name: level_target}, method='nearest').drop_vars(coord_name)
                va_level = ds['va'].sel({coord_name: level_target}, method='nearest').drop_vars(coord_name)
                
                # Get the actual level that was selected
                actual_level = ds[coord_name].sel({coord_name: level_target}, method='nearest').values
                actual_level_hpa = actual_level / scale_factor if scale_factor > 1 else actual_level
                
                print(f"Selected level for {level_hpa} hPa: {actual_level} ({actual_level_hpa:.0f} hPa)")
                
                # Check if the data is empty
                if ua_level.size == 0 or va_level.size == 0:
                    print(f"Warning: No data found for ua/va at {level_hpa} hPa in {input_file}.")
                    continue
                
                # Add to the dataset with level in variable name
                data_vars[f'ua{level_hpa}'] = ua_level
                data_vars[f'va{level_hpa}'] = va_level
                
            except (KeyError, ValueError) as e:
                print(f"Warning: Could not extract level {level_hpa} hPa from {input_file}: {e}")
                continue
        
        if not data_vars:
            print(f"Warning: No valid data extracted from {input_file}.")
            return
        
        # Create a new dataset with the extracted data
        new_ds = xr.Dataset(data_vars)

        # Write the new dataset to the output file
        new_ds.to_netcdf(output_file)
        print(f"Extracted {len(data_vars)//2} levels to {output_file}")

def parse_levels(levels_str):
    """
    Parse a string representation of levels into a list of integers.
    
    Parameters:
    levels_str (str): Comma-separated levels (e.g., "1000,925,850,700,500,300,250")
    
    Returns:
    list: List of integer pressure levels
    """
    try:
        levels = [int(level.strip()) for level in levels_str.split(',')]
        return levels
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid levels format: {e}")

def main():
    """
    Main function to handle command line arguments and call the extract function.
    """
    parser = argparse.ArgumentParser(
        description='Extract wind data (ua and va) at specified pressure levels from 3D fields.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Extract 850 hPa winds (backward compatibility)
  python extract_ua_va_levels.py input.nc output_dir/

  # Extract multiple levels
  python extract_ua_va_levels.py input.nc output_dir/ --levels 1000,925,850,700,500

  # Extract from multiple files with wildcards
  python extract_ua_va_levels.py "data/*.nc" output_dir/ --levels 850,700,500
        """
    )
    
    parser.add_argument('input_files', 
                       help='Input files (can use wildcards or comma-separated list)')
    parser.add_argument('output_path', 
                       help='Output directory path')
    parser.add_argument('--levels', '-l', 
                       type=parse_levels,
                       default='850',
                       help='Comma-separated list of pressure levels in hPa (default: 850)')
    
    args = parser.parse_args()

    # Process file patterns (support wildcards and comma-separated lists)
    all_files = []
    for pattern in args.input_files.split(','):
        matching_files = glob.glob(pattern.strip())
        if not matching_files:
            print(f"Warning: No files match pattern '{pattern}'")
            continue
        all_files.extend(matching_files)
    
    # Check if any files were found
    if not all_files:
        print("Error: No valid input files found.")
        return 1
        
    print(f"Processing {len(all_files)} files at levels: {args.levels} hPa")
    
    for input_file in all_files:
        # Check if the input file is a valid file
        if not os.path.isfile(input_file):
            print(f"Error: Input file {input_file} does not exist.")
            continue

        # Create the output file name based on the input file name
        levels_suffix = "_".join(map(str, args.levels))
        output_file = os.path.join(args.output_path, 
                                   os.path.basename(input_file).replace('.nc', f'_ua_va_{levels_suffix}.nc'))

        # Extract ua and va data at specified levels
        extract_ua_va_levels(input_file, output_file, args.levels)

    return 0

if __name__ == "__main__":
    # Call the main function
    exit_code = main()
    exit(exit_code if exit_code else 0)
