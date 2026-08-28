import pandas as pd
import argparse

# Parse the storm data text file
def parse_storm_file(file_path, unstructured_mesh=True):
    storm_data = []
    with open(file_path, 'r') as f:
        storm_id = 0
        for line in f:
            line = line.strip()
            if line.startswith("start"):
                # New storm
                storm_id += 1
                num_timesteps, year, month, day, hour = map(int, line.split()[1:])
            else:
                # Storm details
                cols = line.split()
                if unstructured_mesh:
                    storm_data.append({
                        "storm_id": storm_id,
                        "grid_id": int(cols[0]),
                        "lon": float(cols[1]),
                        "lat": float(cols[2]),
                        "year": int(cols[-4]),
                        "month": int(cols[-3]),
                        "day": int(cols[-2]),
                        "hour": int(cols[-1])
                    })
                else:
                    storm_data.append({
                        "storm_id": storm_id,
                        "lon_id": int(cols[0]),
                        "lat_id": int(cols[1]),
                        "lon": float(cols[2]),
                        "lat": float(cols[3]),
                        "year": int(cols[-4]),
                        "month": int(cols[-3]),
                        "day": int(cols[-2]),
                        "hour": int(cols[-1])
                    })
    return pd.DataFrame(storm_data)


def find_overlapping_etc_storms(tc_file, etc_file, unstructured_mesh=True):
    """
    Identify ETC storm IDs that overlap with any TC in space and time.
    
    Args:
        tc_file: Path to tropical cyclone file
        etc_file: Path to extratropical cyclone file
        unstructured_mesh: Boolean indicating mesh type
        
    Returns:
        Set of ETC storm IDs that should be filtered out
    """
    # Parse both files
    print("Parsing TC file...")
    tc_df = parse_storm_file(tc_file, unstructured_mesh)
    print(f"Found {tc_df['storm_id'].nunique()} TC storms")
    
    print("Parsing ETC file...")
    etc_df = parse_storm_file(etc_file, unstructured_mesh)
    print(f"Found {etc_df['storm_id'].nunique()} ETC storms")
    
    # Create a set of (lon, lat, year, month, day, hour) tuples for all TC points
    tc_points = set(zip(tc_df['lon'], tc_df['lat'], 
                        tc_df['year'], tc_df['month'], 
                        tc_df['day'], tc_df['hour']))
    print(f"Total TC timesteps: {len(tc_points)}")
    
    # Find ETC storms that have any overlap with TC points
    overlapping_etc_ids = set()
    
    for _, row in etc_df.iterrows():
        point = (row['lon'], row['lat'], row['year'], 
                 row['month'], row['day'], row['hour'])
        if point in tc_points:
            overlapping_etc_ids.add(row['storm_id'])
    
    print(f"Found {len(overlapping_etc_ids)} ETC storms overlapping with TCs")
    
    return overlapping_etc_ids


def filter_etc_file(etc_file, output_file, overlapping_storm_ids):
    """
    Write filtered ETC file, excluding storms with IDs in overlapping_storm_ids.
    Maintains exact formatting of original file.
    
    Args:
        etc_file: Path to input ETC file
        output_file: Path to output filtered file
        overlapping_storm_ids: Set of storm IDs to exclude
    """
    with open(etc_file, 'r') as infile, open(output_file, 'w') as outfile:
        storm_id = 0
        current_storm_lines = []
        skip_current_storm = False
        
        for line in infile:
            if line.strip().startswith("start"):
                # Write previous storm if it wasn't flagged for removal
                if current_storm_lines and not skip_current_storm:
                    outfile.writelines(current_storm_lines)
                
                # Start new storm
                storm_id += 1
                skip_current_storm = storm_id in overlapping_storm_ids
                current_storm_lines = [line]
            else:
                current_storm_lines.append(line)
        
        # Don't forget the last storm
        if current_storm_lines and not skip_current_storm:
            outfile.writelines(current_storm_lines)
    
    print(f"Filtered ETC file written to {output_file}")
    print(f"Removed {len(overlapping_storm_ids)} storms")


def main():
    parser = argparse.ArgumentParser(
        description='Filter ETC file to remove storms that overlap with TCs in space and time.'
    )
    parser.add_argument('tc_file', help='Path to tropical cyclone file')
    parser.add_argument('etc_file', help='Path to extratropical cyclone file')
    parser.add_argument('output_file', help='Path to output filtered ETC file')
    parser.add_argument('--structured', action='store_true', 
                        help='Use if grid is structured (default: unstructured)')
    
    args = parser.parse_args()
    
    unstructured_mesh = not args.structured
    
    # Find overlapping storms
    overlapping_ids = find_overlapping_etc_storms(
        args.tc_file, 
        args.etc_file, 
        unstructured_mesh
    )
    
    # Write filtered output
    filter_etc_file(args.etc_file, args.output_file, overlapping_ids)
    
    print("\nFiltering complete!")


if __name__ == "__main__":
    main()
