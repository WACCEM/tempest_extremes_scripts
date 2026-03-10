import xarray as xr
import numpy as np
import pandas as pd
import os
from dask.diagnostics import ProgressBar

# Parse the storm data text file
def parse_storm_file(file_path):
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

def assign_storm_ids(storm_df, binary_masks, tag_name='ETC_binary_tag', gcd_thresh=1010000):
    """Optimized version that processes by time step instead of by storm"""
    
    # Initialize output mask
    output_mask = (0 * binary_masks[tag_name]).astype(int)
    
    # Group storms by time for batch processing
    storm_df['time_string'] = storm_df.apply(
        lambda storm: pd.to_datetime(f"{int(storm['year']):04d}-{int(storm['month']):02d}-{int(storm['day']):02d}T{int(storm['hour']):02d}", 
                                     format='%Y-%m-%dT%H'),
        axis=1
    )
    storms_by_time = storm_df.groupby('time_string')
    
    # Process each time step once
    for time_string, time_storms in storms_by_time:
        if time_string not in binary_masks['time'].values:
            continue
            
        # Get the mask for this time step (only load once per time)
        time_mask = binary_masks[tag_name].sel(time=time_string)
        time_output = time_mask * 0  # Initialize this timestep's output
        
        # Process all storms for this timestep together
        for _, storm in time_storms.iterrows():
            # Vectorized distance calculation
            distances = sphere_distance(
                time_mask['lon'], time_mask['lat'], 
                storm['lon'], storm['lat']
            )
            
            # Create storm-specific mask
            storm_mask = (time_mask == 1) & (distances <= gcd_thresh)
            
            # Assign storm ID where mask is True
            time_output = time_output.where(~storm_mask, storm['storm_id'])
        
        # Assign the complete time slice back to output
        output_mask.loc[dict(time=time_string)] = time_output.where(time_output != 0, output_mask.sel(time=time_string))
    
    return output_mask

if __name__ == "__main__":
    datadir    = '/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/screamv2_ne120_tracking/'
    storm_file = os.path.join(datadir, 'screamv2_ne120_hp8.etc_stitched_nodes.txt')
    #storm_file = os.path.join(datadir, 'test_nodes_1month.txt')
    storm_df   = parse_storm_file(storm_file)

    # Open the netCDF files using xarray and dask
    binary_masks = xr.open_mfdataset(os.path.join(datadir, "ETC_filt_nodes_screamv2_ne120_hp8_hp8.*.nc"), chunks={'time': 8},
                                 combine='nested', concat_dim='time').unify_chunks()
    #binary_masks = xr.open_dataset(os.path.join(datadir, "ETC_filt_nodes_screamv2_ne120_hp8_hp8.201908.nc"))

    # with ProgressBar():
    #     integer_masks = assign_storm_ids(storm_df, binary_masks)

    binary_masks['ETC_int_tag'] = assign_storm_ids(storm_df, binary_masks)
    #binary_masks = binary_masks.chunk({'time':24, 'cell':786432})
    
    # Write out the result
    binary_masks.to_netcdf(os.path.join(datadir, "updated_masks.nc"), mode='w')
