#!/bin/python

import os
import glob
import re
import argparse

def generate_file_list(output_file, input_config):
    """
    Generate a text file with semicolon-separated lists of files.
    Each line will contain matched files across different patterns that share a common identifier.
    
    Args:
        patterns (list): List of glob patterns for different file types
        output_file (str): Path to the output text file
        identifier_regex (str): Regular expression to extract common identifiers from filenames
        static_file (str, optional): Path to a static file to append to each line
        matching_mode (str): Either 'simple' (original behavior) or 'era5_datalake' (for NERSC ERA5 datalake)
    """

    matching_mode    = input_config.get('matching_mode', 'simple')
    patterns         = input_config['patterns']
    identifier_regex = input_config['pattern_match']
    static_file      = input_config.get('static_file', None)
    era5_start_month = input_config.get('era5_start_month', '195001')
    era5_final_month = input_config.get('era5_final_month', '202412')

    # Collect all files by pattern
    all_files = []
    for pattern in patterns:
        files = sorted(glob.glob(pattern))
        all_files.append(files)
        print(f"Found {len(files)} files matching pattern: {pattern}")
    
    # If any pattern has no matches, exit early
    if any(len(files) == 0 for files in all_files):
        print("Error: One or more patterns didn't match any files.")
        return
    
    if matching_mode == 'simple':
        # Original simple matching logic
        _simple_matching(all_files, identifier_regex, patterns, output_file, static_file)
    elif matching_mode == 'era5_datalake':
        # New temporal matching logic for ERA5 data in the NERSC datalake
        _era5_datalake_matching(output_file, start_month=era5_start_month, final_month=era5_final_month, 
                                ERA5DIR='/global/cfs/cdirs/m3522/cmip6/ERA5/', static_file=static_file,
                                variables_pl=input_config['variables_pl'], 
                                variables_sfc=input_config['variables_sfc'],
                                variables_vinteg=input_config['variables_vinteg'],
                                tp_timescale=input_config.get('tp_timescale', None))
    else:
        raise ValueError(f"Unknown matching_mode: {matching_mode}")

def _simple_matching(all_files, identifier_regex, patterns, output_file, static_file=None):
    """Original simple matching logic"""
    files_by_id = {}
    
    # Process the first pattern
    for file_path in all_files[0]:
        id_match = re.search(identifier_regex, os.path.basename(file_path))
        if id_match:
            identifier = id_match.group(1)
            files_by_id[identifier] = [file_path]
    
    # For each subsequent pattern, find matching files by identifier
    for pattern_idx in range(1, len(patterns)):
        for file_path in all_files[pattern_idx]:
            id_match = re.search(identifier_regex, os.path.basename(file_path))
            if id_match:
                identifier = id_match.group(1)
                if identifier in files_by_id:
                    files_by_id[identifier].append(file_path)
    
    # Filter to keep only complete matches
    complete_matches = {id_val: files for id_val, files in files_by_id.items() 
                       if len(files) == len(patterns)}
    
    # Write to output file
    with open(output_file, 'w') as f:
        for identifier, files in sorted(complete_matches.items()):
            file_line = ";".join(files)
            # Append the static file if provided
            if static_file:
                file_line += ";" + static_file
            f.write(file_line + "\n")
    
    print(f"Generated {len(complete_matches)} paired entries in {output_file}")
    if complete_matches:
        example_line = next(iter(complete_matches.values()))
        if static_file:
            example_line = example_line + [static_file]
        print(f"Example line: {';'.join(example_line)}")
    else:
        print("No complete matches found.")
    
    return complete_matches

def _era5_datalake_matching(output_file, start_month='195001', final_month='202412', ERA5DIR='/global/cfs/cdirs/m3522/cmip6/ERA5/',
                            static_file='/pscratch/sd/b/beharrop/kmscale_hackathon/ERA5_tracking/e5.oper.invariant.Zs.ll025sc.nc',
                            variables_pl=['128_129_z'], variables_sfc=['128_151_msl', '128_165_10u', '128_166_10v'],
                            variables_vinteg=['162_071_viwve', '162_072_viwvn'], tp_timescale=None):
    """
    Temporal matching logic for ERA5 data in the NERSC datalake where some files are daily
    and others are monthly. The start_month, final_month, variables_pl, and variables_sfc are
    specified in the config yaml files.
    tp_timescale can be '1h', '3h', or '6h'
    """

    with open(output_file, 'w') as f:
        for yearmonth in sorted(os.listdir(os.path.join(ERA5DIR, 'e5.oper.an.pl' + os.sep))):
            if int(yearmonth) > int(final_month):
                continue
            if int(yearmonth) < int(start_month):
                continue
            sfc_write_line = ''
            for sfc_var in variables_sfc:
                sfc_write_line += ';' + glob.glob(os.path.join(ERA5DIR, 'e5.oper.an.sfc', yearmonth, f"*{sfc_var}*"))[0]
            # sfc_write_line = sfc_write_line[1:]
            for vinteg_var in variables_vinteg:
                sfc_write_line += ';' + glob.glob(os.path.join(ERA5DIR, 'e5.oper.an.vinteg', yearmonth, f"*{vinteg_var}*"))[0]
            if static_file:
                sfc_write_line += ';' + static_file
            if tp_timescale:
                sfc_write_line += ';' + os.path.join(ERA5DIR, f"e5.accumulated_tp_{tp_timescale}", 
                                                   f"e5.accumulated_tp_{tp_timescale}.{yearmonth}.nc")
            if len(variables_pl) > 0:
                for zfile in sorted(glob.glob(os.path.join(ERA5DIR, 'e5.oper.an.pl', yearmonth, '*128_129_z*'))):
                    date_string_pl = zfile[-24:-3]
                    write_line     = sfc_write_line
                    for pl_var in variables_pl:
                        write_line = os.path.join(ERA5DIR, 'e5.oper.an.pl', yearmonth, 
                                                   f"e5.oper.an.pl.{pl_var}.ll025sc.{date_string_pl}.nc") + sfc_write_line
                    f.write(write_line + "\n")
            else:
                write_line = sfc_write_line[1:]
                f.write(write_line + "\n")
    return None

def transform_file_list(input_file, output_file, identifier_regex, prefix="", suffix=""):
    """
    Transform an existing file list by extracting identifiers and creating new filenames.
    
    Args:
        input_file (str): Path to input file with semicolon-separated file lists
        output_file (str): Path to output file where transformed names will be written
        identifier_regex (str): Regular expression to extract identifiers from filenames
        prefix (str): Text to add before the identifier in new filenames
        suffix (str): Text to add after the identifier in new filenames
    """
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found.")
        return
    
    transformed_lines = []
    skipped_lines = 0
    
    with open(input_file, 'r') as f:
        for line in f:
            # Get the first file path from the line (before the first semicolon)
            line = line.strip()
            if not line:
                continue
                
            first_file = line.split(';')[0]
            basename = os.path.basename(first_file)
            
            # Extract the identifier using the regex
            id_match = re.search(identifier_regex, basename)
            if id_match:
                identifier = id_match.group(1)
                new_filename = f"{prefix}{identifier}{suffix}"
                transformed_lines.append(new_filename)
            else:
                skipped_lines += 1
    
    # Write transformed lines to output file
    with open(output_file, 'w') as f:
        for line in transformed_lines:
            f.write(line + '\n')
    
    print(f"Generated {len(transformed_lines)} transformed filenames in {output_file}")
    if skipped_lines > 0:
        print(f"Warning: Skipped {skipped_lines} lines due to no identifier match.")
    if transformed_lines:
        print(f"Example line: {transformed_lines[0]}")
    else:
        print("No transforms generated.")

def main():
    parser = argparse.ArgumentParser(description='Generate or transform file lists for processing.')
    subparsers = parser.add_subparsers(dest='command')
    
    # Subparser for the original generate functionality
    gen_parser = subparsers.add_parser('generate', 
                                       help='Generate a file with semicolon-separated paired paths')
    gen_parser.add_argument('--patterns', required=True, nargs='+', 
                           help='List of glob patterns for different file types')
    gen_parser.add_argument('--output', required=True, help='Output text file path')
    gen_parser.add_argument('--regex', required=True, 
                           help='Regular expression with a capture group to extract identifiers')
    gen_parser.add_argument('--static-file', help='Path to a static file to append to each line')
    gen_parser.add_argument('--matching-mode', choices=['simple', 'era5_datalake'], default='simple',
                           help='Matching mode: simple (exact identifier match) or era5_datalake (time-hierarchical matching for ERA5 data)')
    
    # Subparser for the new transform functionality
    trans_parser = subparsers.add_parser('transform', 
                                        help='Transform an existing file list to new filenames')
    trans_parser.add_argument('--input', required=True, help='Input file with paired paths')
    trans_parser.add_argument('--output', required=True, help='Output file for transformed names')
    trans_parser.add_argument('--regex', required=True, 
                             help='Regular expression with a capture group to extract identifiers')
    trans_parser.add_argument('--prefix', default='', help='Text to add before the identifier')
    trans_parser.add_argument('--suffix', default='', help='Text to add after the identifier')
    
    args = parser.parse_args()
    
    if args.command == 'generate':
        generate_file_list(args.patterns, args.output, args.regex, args.static_file, args.matching_mode)
    elif args.command == 'transform':
        transform_file_list(args.input, args.output, args.regex, args.prefix, args.suffix)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()