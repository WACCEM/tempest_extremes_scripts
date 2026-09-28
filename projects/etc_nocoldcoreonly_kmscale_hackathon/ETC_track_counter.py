import xarray as xr
import numpy as np
import pandas as pd
import os
import sys
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

def sphere_distance(lon1=0., lat1=0., lon2=0., lat2=0., units='degrees', radius=6.37122e6):
    if units.lower() in ['degrees', 'deg', 'd']:
        lon1 = np.deg2rad(lon1)
        lat1 = np.deg2rad(lat1)
        lon2 = np.deg2rad(lon2)
        lat2 = np.deg2rad(lat2)
    elif units.lower() in ['radians', 'rad', 'r']:
        pass
    else:
        raise KeyError("Unrecognized value for units (must be degrees or radians)")
    distance = np.arccos( np.sin(lat1) * np.sin(lat2) + np.cos(lat1) * np.cos(lat2) * np.cos(lon2 - lon1) ) * radius
    return distance

def build_sample_grid_template(sample_grid_file, start_time, end_time, time_increment):
    """Build a zero-valued track array from a static grid and requested times."""
    try:
        start = pd.to_datetime(start_time, format='%Y-%m-%dT%H')
        end = pd.to_datetime(end_time, format='%Y-%m-%dT%H')
    except (TypeError, ValueError) as error:
        raise ValueError('Times must use YYYY-MM-DDTHH format') from error
    if start.strftime('%Y-%m-%dT%H') != start_time or end.strftime('%Y-%m-%dT%H') != end_time:
        raise ValueError('Times must use YYYY-MM-DDTHH format')

    try:
        increment = pd.Timedelta(time_increment)
    except (TypeError, ValueError) as error:
        raise ValueError('Time increment must be a positive whole-hour duration, such as 6h') from error

    one_hour = pd.Timedelta(hours=1)
    if increment <= pd.Timedelta(0) or increment % one_hour != pd.Timedelta(0):
        raise ValueError('Time increment must be a positive whole-hour duration, such as 6h')
    if end < start:
        raise ValueError('End time must be at or after start time')

    times = pd.date_range(start=start, end=end, freq=increment)
    if len(times) == 0 or times[-1] != end:
        raise ValueError('End time must fall on the requested time increment')

    with xr.open_dataset(sample_grid_file) as sample_grid:
        coordinate_names = None
        for longitude_name, latitude_name in (('lon', 'lat'), ('longitude', 'latitude')):
            if longitude_name in sample_grid and latitude_name in sample_grid:
                coordinate_names = longitude_name, latitude_name
                break
        if coordinate_names is None:
            raise ValueError('Sample grid must contain lon/lat or longitude/latitude')

        longitude_name, latitude_name = coordinate_names
        longitude = sample_grid[longitude_name].load()
        latitude = sample_grid[latitude_name].load()
        longitude_dims = set(longitude.dims)
        latitude_dims = set(latitude.dims)
        if not longitude_dims or not latitude_dims:
            raise ValueError('Longitude and latitude coordinates must have spatial dimensions')
        if longitude_dims & latitude_dims and longitude_dims != latitude_dims:
            raise ValueError('Longitude and latitude must share the same grid dimensions or use separate axes')
        latitude_grid, _ = xr.broadcast(latitude, longitude)
        spatial_dims = latitude_grid.dims

        spatial_coords = {
            name: coord.load()
            for name, coord in sample_grid.coords.items()
            if name != 'time' and set(coord.dims).issubset(spatial_dims)
        }
        spatial_coords[longitude_name] = longitude
        spatial_coords[latitude_name] = latitude

        output = xr.DataArray(
            np.zeros((len(times), *latitude_grid.shape), dtype=np.int64),
            dims=('time', *spatial_dims),
            coords={'time': times, **spatial_coords},
            name='track_ids'
        )
        output.attrs.update(sample_grid.attrs)

    return output


def assign_storm_ids(storm_df, binary_masks, tag_name='ETC_binary_tag', gcd_thresh=1010000, sample_grid=False):
    """Assign storm IDs using either a binary mask or a sample-grid template."""
    if sample_grid:
        output_mask = binary_masks.astype(int)
    else:
        output_mask = (0 * binary_masks[tag_name]).astype(int)

    storm_df['time_string'] = storm_df.apply(
        lambda storm: pd.to_datetime(
            f"{int(storm['year']):04d}-{int(storm['month']):02d}-{int(storm['day']):02d}T{int(storm['hour']):02d}",
            format='%Y-%m-%dT%H'
        ),
        axis=1
    )
    storms_by_time = storm_df.groupby('time_string')

    for time_string, time_storms in storms_by_time:
        if time_string not in output_mask['time'].values:
            continue

        if sample_grid:
            time_mask = output_mask.sel(time=time_string)
        else:
            time_mask = binary_masks[tag_name].sel(time=time_string)
        time_output = time_mask * 0

        for _, storm in time_storms.iterrows():
            try:
                distances = sphere_distance(
                    time_mask['lon'], time_mask['lat'],
                    storm['lon'], storm['lat']
                )
            except KeyError:
                distances = sphere_distance(
                    time_mask['longitude'], time_mask['latitude'],
                    storm['lon'], storm['lat']
                )

            if sample_grid:
                storm_mask = distances <= gcd_thresh
            else:
                storm_mask = (time_mask == 1) & (distances <= gcd_thresh)
            time_output = time_output.where(~storm_mask, storm['storm_id'])

        output_mask.loc[dict(time=time_string)] = time_output.where(
            time_output != 0, output_mask.sel(time=time_string)
        )

    return output_mask

def main():
    """
    Main function to handle command line arguments and assign the storm IDs
    """
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Assign storm IDs to binary masks based on storm track data')
    parser.add_argument('storm_file', help='Path to the storm track file')
    parser.add_argument('binary_masks_file', nargs='?', default=None,
                        help='Path to the binary masks netCDF file (legacy mode)')
    parser.add_argument('legacy_output_file', nargs='?', default=None,
                        help='Path for the output netCDF file (legacy mode)')
    parser.add_argument('--unstructured', action='store_true', default=True,
                        help='Use unstructured mesh format (default: True)')
    parser.add_argument('--structured', dest='unstructured', action='store_false',
                        help='Use structured (lat-lon) grid format')
    parser.add_argument('--gcd_threshold', type=float, default=9.0,
                        help='Great circle distance threshold in degrees (default: 9)')
    parser.add_argument('--stormtype', type=str, default='ETC', choices=['ETC', 'TC'],
                        help='Storm type: ETC or TC (default: ETC)')
    parser.add_argument('--binary_tag_name', type=str, default=None,
                        help='Name of the binary mask variable (default: <stormtype>_binary_tag)')
    parser.add_argument('--tracks-only', action='store_true',
                        help='Write only the storm ID variable and its coordinates')
    parser.add_argument('--sample_grid_file',
                        help='Sample NetCDF file providing the grid for a new track-only output')
    parser.add_argument('--start_time', help='Inclusive start time in YYYY-MM-DDTHH format')
    parser.add_argument('--end_time', help='Inclusive end time in YYYY-MM-DDTHH format')
    parser.add_argument('--time_increment', help='Positive whole-hour increment, such as 6h')
    parser.add_argument('--output_file', help='Output path for sample-grid mode')

    args = parser.parse_args()
    sample_mode = args.sample_grid_file is not None
    time_args = (args.start_time, args.end_time, args.time_increment)
    if sample_mode:
        if args.binary_masks_file is not None or args.legacy_output_file is not None:
            parser.error('Do not supply positional binary-mask/output files with --sample_grid_file')
        if any(value is None for value in time_args) or args.output_file is None:
            parser.error('--sample_grid_file requires --start_time, --end_time, --time_increment, and --output_file')
        if args.binary_tag_name is not None:
            parser.error('--binary_tag_name can only be used with a binary mask file')
    else:
        if args.binary_masks_file is None or args.legacy_output_file is None:
            parser.error('Legacy mode requires storm_file, binary_masks_file, and output_file positionally')
        if any(value is not None for value in time_args) or args.output_file is not None:
            parser.error('Time options and --output_file require --sample_grid_file')
    storm_df = parse_storm_file(args.storm_file, unstructured_mesh=args.unstructured)
    gcd_thresh = sphere_distance(lon1=0, lat1=0, lon2=args.gcd_threshold, lat2=0, units='degrees')
    int_tag_name = f"{args.stormtype}_int_tag"

    if sample_mode:
        try:
            sample_template = build_sample_grid_template(
                args.sample_grid_file, args.start_time, args.end_time, args.time_increment
            )
        except ValueError as error:
            parser.error(str(error))
        assigned_track_ids = assign_storm_ids(
            storm_df, sample_template, gcd_thresh=gcd_thresh, sample_grid=True
        )
        output_dataset = assigned_track_ids.rename(int_tag_name).to_dataset()
        output_dataset.attrs.update(sample_template.attrs)
        output_path = args.output_file
    else:
        binary_masks = xr.open_dataset(args.binary_masks_file)
        binary_tag_name = args.binary_tag_name or f"{args.stormtype}_binary_tag"
        assigned_track_ids = assign_storm_ids(
            storm_df, binary_masks, tag_name=binary_tag_name, gcd_thresh=gcd_thresh
        )
        if args.tracks_only:
            output_dataset = assigned_track_ids.rename(int_tag_name).to_dataset()
            output_dataset.attrs.update(binary_masks.attrs)
        else:
            binary_masks[int_tag_name] = assigned_track_ids
            output_dataset = binary_masks
        output_path = args.legacy_output_file

    output_dataset.to_netcdf(output_path, mode='w')
    

if __name__ == "__main__":
    # Usage examples:
    # python ETC_track_counter.py storm_file.txt binary_masks.nc output.nc
    # python ETC_track_counter.py storm_file.txt binary_masks.nc output.nc --structured
    main()
    
