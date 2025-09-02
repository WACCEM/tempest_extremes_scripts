#!/bin/python

import os
import glob
import re
import argparse

def generate_file_list(patterns, output_file, identifier_regex, static_file=None):
    """
    Generate a text file with semicolon-separated lists of files.
    Each line will contain matched files across different patterns that share a common identifier.
    
    Args:
        patterns (list): List of glob patterns for different file types
        output_file (str): Path to the output text file
        identifier_regex (str): Regular expression to extract common identifiers from filenames
        static_file (str, optional): Path to a static file to append to each line
    """
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
    
    # Create a mapping of identifiers to file lists
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
        generate_file_list(args.patterns, args.output, args.regex, args.static_file)
    elif args.command == 'transform':
        transform_file_list(args.input, args.output, args.regex, args.prefix, args.suffix)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()