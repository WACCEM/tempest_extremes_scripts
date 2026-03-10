#!/bin/bash

# Script to remove ELEV variable from netCDF files
# Usage: ./scrub_ELEV_from_icon.sh /path/to/input_dir /path/to/output_dir

# Check if input and output directories are provided
if [ "$#" -lt 2 ]; then
    echo "Usage: $0 input_directory output_directory"
    exit 1
fi

INPUT_DIR="$1"
OUTPUT_DIR="$2"

# Check if input directory exists
if [ ! -d "$INPUT_DIR" ]; then
    echo "Error: Input directory does not exist: $INPUT_DIR"
    exit 1
fi

# Check if ncks is available, and load if needed
if ! command -v ncks &> /dev/null; then
    echo "ncks not found in PATH. Loading E3SM environment..."
    source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh
    
    # Verify ncks is now available
    if ! command -v ncks &> /dev/null; then
        echo "Error: Failed to load ncks. Please ensure the environment script is correct."
        exit 1
    else
        echo "Successfully loaded ncks."
    fi
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Count files for progress tracking
total_files=$(find "$INPUT_DIR" -name "*.nc" | wc -l)
processed=0

echo "Found $total_files netCDF files to process."
echo "Removing ELEV variable from all files..."

# Process each netCDF file
for file in "$INPUT_DIR"/*.nc; do
    if [ -f "$file" ]; then
        filename=$(basename "$file")
        output_file="$OUTPUT_DIR/$filename"
        
        # Check if output file already exists
        if [ -f "$output_file" ]; then
            echo "Skipping ($((processed+1))/$total_files): $filename (already exists)"
            ((processed++))
            continue
        fi
        
        echo "Processing ($((processed+1))/$total_files): $filename"
        
        # Remove ELEV variable using NCO tools
        ncks -x -v ELEV "$file" "$output_file"
        
        if [ $? -eq 0 ]; then
            echo "  ✓ Successfully processed: $filename"
        else
            echo "  ✗ Error processing: $filename"
        fi
        
        ((processed++))
    fi
done

echo "Done! Processed $processed files."
echo "Modified files are available in: $OUTPUT_DIR"