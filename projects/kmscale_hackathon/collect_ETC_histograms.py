import xarray as xr
import pandas as pd
import numpy as np
import os
import intake
from easygems import healpix as egh
import warnings
import cftime
import dask
import dask.array as da
from dask.diagnostics import ProgressBar
import argparse
warnings.filterwarnings('ignore')

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
                        "grid_id":  int(cols[0]),
                        "lon":      float(cols[1]),
                        "lat":      float(cols[2]),
                        "slp":      float(cols[3]),
                        "wind":     float(cols[4]),
                        "zs":       float(cols[5]),
                        "pr":       float(cols[6]),
                        "year":     int(cols[-4]),
                        "month":    int(cols[-3]),
                        "day":      int(cols[-2]),
                        "hour":     int(cols[-1])
                    })
                else:
                    storm_data.append({
                        "storm_id": storm_id,
                        "lon_id":   int(cols[0]),
                        "lat_id":   int(cols[1]),
                        "lon":      float(cols[2]),
                        "lat":      float(cols[3]),
                        "slp":      float(cols[4]),
                        "wind":     float(cols[5]),
                        "zs":       float(cols[6]),
                        "pr":       float(cols[7]),
                        "year":     int(cols[-4]),
                        "month":    int(cols[-3]),
                        "day":      int(cols[-2]),
                        "hour":     int(cols[-1])
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

def compute_histogram_for_single_observation(
    wind_data: da.Array,
    lon_data: da.Array,
    lat_data: da.Array,
    storm_lon: float,
    storm_lat: float,
    sphere_distance: callable,
    gcd_threshold: float,
    bins: np.ndarray
) -> np.ndarray:
    """
    Compute histogram for a single storm observation.
    This function will be wrapped in dask.delayed.
    
    Parameters:
    -----------
    wind_data : da.Array
        Wind speed data (1D array for unstructured mesh)
    lon_data : da.Array
        Longitude coordinates
    lat_data : da.Array
        Latitude coordinates
    storm_lon : float
        Storm center longitude
    storm_lat : float
        Storm center latitude
    sphere_distance : callable
        Function to compute great circle distance
    gcd_threshold : float
        Distance threshold in degrees
    bins : np.ndarray
        Histogram bin edges
    
    Returns:
    --------
    np.ndarray : Histogram counts
    """
    # Compute distances (assuming sphere_distance can handle dask arrays)
    distances = sphere_distance(
        lon1=storm_lon, 
        lat1=storm_lat,
        lon2=lon_data, 
        lat2=lat_data
    )
    
    # If distances is a dask array, compute it
    if hasattr(distances, 'compute'):
        distances = distances.compute()
    
    # If wind_data is a dask array, compute it
    if hasattr(wind_data, 'compute'):
        wind_data = wind_data.compute()
    
    # Mask wind data by distance
    wind_within_radius = wind_data[distances < gcd_threshold]
    
    # Remove NaN values
    wind_within_radius = wind_within_radius[np.isfinite(wind_within_radius)]
    
    # Compute histogram
    if len(wind_within_radius) > 0:
        hist_counts, _ = np.histogram(wind_within_radius, bins=bins)
    else:
        hist_counts = np.zeros(len(bins) - 1, dtype=np.int64)
    
    return hist_counts


def prepare_timestamps(df_tracks, ds):
    """
    Create timestamps compatible with the dataset's calendar.
    Handles both cftime and datetime64 calendars automatically.
    
    Parameters:
    -----------
    df_tracks : pd.DataFrame
        DataFrame with year, month, day, hour columns
    ds : xr.Dataset
        Dataset with time dimension
    
    Returns:
    --------
    list : Timestamps matching the dataset's time type
    """
    # Check the type of the first time value
    first_time = ds['time'].values[0]
    time_type = type(first_time)
    
    print(f"Detected time type: {time_type}")
    
    # Check if it's a numpy datetime64
    if isinstance(first_time, np.datetime64):
        print("Using pandas/numpy datetime64 timestamps")
        
        # Use pandas Timestamp (compatible with datetime64)
        timestamps = []
        for _, row in df_tracks.iterrows():
            try:
                ts = pd.Timestamp(
                    year=int(row['year']),
                    month=int(row['month']),
                    day=int(row['day']),
                    hour=int(row['hour'])
                )
                timestamps.append(ts)
            except Exception as e:
                print(f"Warning: Could not create timestamp for {row[['year','month','day','hour']].values}: {e}")
                timestamps.append(None)
    
    # Check if it's a cftime object
    elif hasattr(first_time, 'calendar'):
        calendar = first_time.calendar
        print(f"Using cftime timestamps with calendar: {calendar}")
        
        # Use cftime objects
        timestamps = []
        for _, row in df_tracks.iterrows():
            try:
                ts = cftime.datetime(
                    int(row['year']),
                    int(row['month']),
                    int(row['day']),
                    int(row['hour']),
                    0, 0, 0,
                    calendar=calendar
                )
                timestamps.append(ts)
            except Exception as e:
                print(f"Warning: Could not create timestamp for {row[['year','month','day','hour']].values}: {e}")
                timestamps.append(None)
    
    else:
        # Fallback: try to get calendar from encoding
        try:
            calendar = ds['time'].encoding.get('calendar', 'standard')
            print(f"Using cftime timestamps with calendar from encoding: {calendar}")
            
            timestamps = []
            for _, row in df_tracks.iterrows():
                try:
                    ts = cftime.datetime(
                        int(row['year']),
                        int(row['month']),
                        int(row['day']),
                        int(row['hour']),
                        0, 0, 0,
                        calendar=calendar
                    )
                    timestamps.append(ts)
                except Exception as e:
                    print(f"Warning: Could not create timestamp for {row[['year','month','day','hour']].values}: {e}")
                    timestamps.append(None)
        except Exception as e:
            print(f"Error: Could not determine calendar type: {e}")
            print("Falling back to pandas Timestamp")
            
            timestamps = []
            for _, row in df_tracks.iterrows():
                try:
                    ts = pd.Timestamp(
                        year=int(row['year']),
                        month=int(row['month']),
                        day=int(row['day']),
                        hour=int(row['hour'])
                    )
                    timestamps.append(ts)
                except Exception as e:
                    print(f"Warning: Could not create timestamp for {row[['year','month','day','hour']].values}: {e}")
                    timestamps.append(None)
    
    return timestamps


def cftime_to_numeric(timestamps_array, reference_date=None):
    """
    Convert timestamps (cftime or datetime64) to numeric values (hours since reference).
    None values are converted to NaN.
    
    Parameters:
    -----------
    timestamps_array : np.ndarray
        Array of timestamp objects (cftime, datetime64, or pd.Timestamp), possibly with None values
    reference_date : timestamp object, optional
        Reference date for conversion. If None, uses first valid timestamp.
    
    Returns:
    --------
    tuple : (numeric_array, reference_date)
        numeric_array : np.ndarray with hours since reference (NaN for missing)
        reference_date : the reference timestamp used
    """
    flat_stamps = timestamps_array.flatten()
    
    # Find first valid timestamp for reference if not provided
    if reference_date is None:
        for ts in flat_stamps:
            if ts is not None:
                reference_date = ts
                break
    
    if reference_date is None:
        raise ValueError("No valid timestamps found in array")
    
    # Convert to numeric
    numeric_array = np.full(timestamps_array.shape, np.nan, dtype=np.float64)
    
    # Determine if we're working with pandas/numpy timestamps or cftime
    is_pandas_like = isinstance(reference_date, (pd.Timestamp, np.datetime64))
    
    if is_pandas_like:
        # Convert reference to pd.Timestamp for consistency
        if isinstance(reference_date, np.datetime64):
            reference_date = pd.Timestamp(reference_date)
        
        for idx, ts in np.ndenumerate(timestamps_array):
            if ts is not None:
                try:
                    # Convert to pd.Timestamp if needed
                    if isinstance(ts, np.datetime64):
                        ts = pd.Timestamp(ts)
                    
                    # Calculate hours since reference
                    delta = ts - reference_date
                    numeric_array[idx] = delta.total_seconds() / 3600.0
                except Exception as e:
                    print(f"Warning: Could not convert timestamp at {idx}: {e}")
    
    else:
        # Working with cftime objects
        for idx, ts in np.ndenumerate(timestamps_array):
            if ts is not None:
                try:
                    # Calculate hours since reference
                    delta = ts - reference_date
                    numeric_array[idx] = delta.total_seconds() / 3600.0
                except AttributeError:
                    # Fallback for cftime objects that don't support direct subtraction
                    # This is a crude approximation
                    try:
                        numeric_array[idx] = (
                            (ts.year - reference_date.year) * 8760 +
                            (ts.month - reference_date.month) * 730 +
                            (ts.day - reference_date.day) * 24 +
                            (ts.hour - reference_date.hour)
                        )
                    except Exception as e:
                        print(f"Warning: Could not convert timestamp at {idx}: {e}")
    
    return numeric_array, reference_date


def compute_storm_wind_histograms_dask(
    df_tracks: pd.DataFrame,
    ds: xr.Dataset,
    sphere_distance: callable,
    gcd_threshold: float = 10.0,
    bins: np.ndarray = np.arange(0, 51),
    batch_size: int = 50
) -> xr.Dataset:
    """
    Compute wind speed histograms for all storm tracks using dask for efficiency.
    Handles cftime calendars properly.
    Optimized for unstructured mesh data (time x cell dimensions).
    
    Parameters:
    -----------
    df_tracks : pd.DataFrame
        Storm track data with columns: storm_id, lon, lat, year, month, day, hour
    ds : xr.Dataset
        Climate dataset with sfcWind, lon, lat, time (dimensions: time x cell)
    sphere_distance : callable
        Function to compute great circle distance
    gcd_threshold : float
        Great circle distance threshold in degrees
    bins : np.ndarray
        Histogram bin edges
    batch_size : int
        Number of storm observations to process in parallel
    
    Returns:
    --------
    xr.Dataset with histogram counts
    """
    
    # Ensure data is sorted
    df_tracks = df_tracks.sort_values(['storm_id', 'year', 'month', 'day', 'hour']).reset_index(drop=True)
    
    # Create proper timestamps
    print("Creating timestamps compatible with dataset calendar...")
    df_tracks['timestamp'] = prepare_timestamps(df_tracks, ds)
    
    # Remove any rows where timestamp creation failed
    valid_times = df_tracks['timestamp'].notna()
    if not valid_times.all():
        print(f"Warning: Removing {(~valid_times).sum()} rows with invalid timestamps")
        df_tracks = df_tracks[valid_times].reset_index(drop=True)
    
    n_observations = len(df_tracks)
    n_bins = len(bins) - 1
    
    print(f"Processing {n_observations} storm observations...")
    print(f"Dataset dimensions: {ds.dims}")
    print(f"Example timestamp from tracks: {df_tracks['timestamp'].iloc[0]}")
    print(f"Example timestamp from dataset: {ds['time'].values[0]}")
    
    # Pre-extract lon/lat
    if 'time' in ds['lon'].dims:
        print("Warning: lon/lat vary with time - using first time step for coordinates")
        lon_grid = ds['lon'].isel(time=0)
        lat_grid = ds['lat'].isel(time=0)
    else:
        lon_grid = ds['lon']
        lat_grid = ds['lat']
    
    # Build list of delayed computations
    delayed_histograms = []
    observation_metadata = []
    
    for idx, row in df_tracks.iterrows():
        if idx % 100 == 0:
            print(f"  Preparing computation {idx}/{n_observations}")
        
        # Select wind data for this time - now timestamps match!
        try:
            wind_at_time = ds['sfcWind'].sel(time=row['timestamp'])
        except KeyError as e:
            print(f"  Warning: Could not find time {row['timestamp']} in dataset: {e}")
            # Try nearest neighbor as fallback
            wind_at_time = ds['sfcWind'].sel(time=row['timestamp'], method='nearest', tolerance='1H')
        
        # Create delayed computation
        delayed_hist = dask.delayed(compute_histogram_for_single_observation)(
            wind_data=wind_at_time.data,
            lon_data=lon_grid.data,
            lat_data=lat_grid.data,
            storm_lon=row['lon'],
            storm_lat=row['lat'],
            sphere_distance=sphere_distance,
            gcd_threshold=gcd_threshold,
            bins=bins
        )
        
        delayed_histograms.append(delayed_hist)
        observation_metadata.append({
            'storm_id': row['storm_id'],
            'timestamp': row['timestamp'],
            'lon': row['lon'],
            'lat': row['lat'],
            'observation_index': idx
        })
    
    # Compute in batches
    print(f"\nComputing histograms in batches of {batch_size}...")
    all_histograms = []
    
    for i in range(0, len(delayed_histograms), batch_size):
        batch_end = min(i + batch_size, len(delayed_histograms))
        print(f"  Computing batch {i//batch_size + 1}/{(len(delayed_histograms)-1)//batch_size + 1} (observations {i}-{batch_end})")
        
        batch = delayed_histograms[i:batch_end]
        with ProgressBar():
            batch_results = dask.compute(*batch)
        all_histograms.extend(batch_results)
    
    # Convert to numpy array
    histogram_array = np.array(all_histograms)
    
    # Reshape into (storm_id, track_time, wind_bin) structure
    storm_ids = df_tracks['storm_id'].unique()
    n_storms = len(storm_ids)
    max_track_length = df_tracks.groupby('storm_id').size().max()
    
    print(f"\nReshaping results: {n_storms} storms, max {max_track_length} time steps")
    
    # Initialize output arrays
    histogram_data = np.zeros((n_storms, max_track_length, n_bins), dtype=np.int64)
    valid_mask = np.zeros((n_storms, max_track_length), dtype=bool)
    
    # Arrays for metadata - use object dtype for cftime
    timestamps = np.full((n_storms, max_track_length), None, dtype=object)
    track_lons = np.full((n_storms, max_track_length), np.nan)
    track_lats = np.full((n_storms, max_track_length), np.nan)
    track_slps = np.full((n_storms, max_track_length), np.nan)
    track_winds = np.full((n_storms, max_track_length), np.nan)
    
    # Fill arrays
    storm_id_to_idx = {sid: idx for idx, sid in enumerate(storm_ids)}
    
    for obs_idx, row in df_tracks.iterrows():
        storm_idx = storm_id_to_idx[row['storm_id']]
        
        # Find time index for this storm
        storm_obs = df_tracks[df_tracks['storm_id'] == row['storm_id']]
        time_idx = (storm_obs.index == obs_idx).argmax()
        
        histogram_data[storm_idx, time_idx, :] = histogram_array[obs_idx]
        valid_mask[storm_idx, time_idx] = True
        timestamps[storm_idx, time_idx] = row['timestamp']
        track_lons[storm_idx, time_idx] = row['lon']
        track_lats[storm_idx, time_idx] = row['lat']
        
        if 'slp' in row:
            track_slps[storm_idx, time_idx] = row['slp']
        if 'wind' in row:
            track_winds[storm_idx, time_idx] = row['wind']

    print("Converting timestamps to numeric format...")
    timestamps_numeric, reference_date = cftime_to_numeric(timestamps)
    
    # Create xarray Dataset
    ds_output = xr.Dataset(
        {
            'histogram_counts': (
                ['storm_id', 'track_time', 'wind_bin'],
                histogram_data,
                {
                    'long_name': 'Wind speed histogram counts within radius',
                    'units': 'count',
                    'gcd_threshold_degrees': gcd_threshold
                }
            ),
            'valid': (
                ['storm_id', 'track_time'],
                valid_mask,
                {'long_name': 'Valid histogram flag'}
            ),
            'timestamp': (
                ['storm_id', 'track_time'],
                timestamps_numeric,
                {
                    'long_name': 'Observation timestamp',
                    'units': f'hours since {reference_date}',
                    'calendar': reference_date.calendar if hasattr(reference_date, 'calendar') else 'noleap',
                    'description': 'Hours since reference date; NaN indicates no observation'
                }
            ),
            'track_lon': (
                ['storm_id', 'track_time'],
                track_lons,
                {'long_name': 'Storm center longitude', 'units': 'degrees_east'}
            ),
            'track_lat': (
                ['storm_id', 'track_time'],
                track_lats,
                {'long_name': 'Storm center latitude', 'units': 'degrees_north'}
            ),
            'track_slp': (
                ['storm_id', 'track_time'],
                track_slps,
                {'long_name': 'Storm center sea level pressure', 'units': 'Pa'}
            ),
            'track_wind': (
                ['storm_id', 'track_time'],
                track_winds,
                {'long_name': 'Storm center wind speed', 'units': 'm/s'}
            ),
        },
        coords={
            'storm_id': ('storm_id', storm_ids, {'long_name': 'Storm identifier'}),
            'track_time': ('track_time', np.arange(max_track_length), 
                          {'long_name': 'Time step along track'}),
            'wind_bin_center': ('wind_bin', (bins[:-1] + bins[1:]) / 2,
                               {'long_name': 'Wind bin center', 'units': 'm/s'}),
            'wind_bin_edge': ('wind_bin_edge', bins,
                             {'long_name': 'Wind bin edges', 'units': 'm/s'}),
        },
        attrs={
            'description': 'Wind speed histograms for extratropical cyclone tracks',
            'gcd_threshold': f'{gcd_threshold} degrees',
            'n_storms': n_storms,
            'total_observations': n_observations,
        }
    )
    
    return ds_output



if __name__ == "__main__":

    # Parse command-line arguments
    parser = argparse.ArgumentParser(
        description='Compute wind speed histograms for extratropical cyclone tracks',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('--case', type=str, default=None,
                        choices=['era5', 'casesm2', 'icon', 'nicam', 'scream', 'um', 'conus404'],
                        help='Case to process. If not specified, all cases will be processed.')
    parser.add_argument('--zoom_level', type=int, default=5,
                        help='HEALPix zoom level for data resolution.')
    args = parser.parse_args()

    parent_dir = '/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/'
    output_dir = os.path.join(parent_dir, 'ETC_histograms' + os.sep)
    cases      = dict(era5    = dict(file='era5_tracking_full/era5.etc_stitched_nodes.txt', cat_name='ERA5',
                                     unstructured=False, start='201901', final='202112', start_date='1979-01-01', final_date='2021-12-31',
                                     data_frequency='default',
                                     wind_vars=('10u', '10v'),
                                     syclops_file='SyCLoPS_classified_ERA5.parquet'),
                      casesm2 = dict(file='casesm2_10km_nocumulus_tracking/casesm2_10km_nocumulus_hp8.etc_stitched_nodes.txt', cat_name='casesm2_10km_nocumulus',
                                     unstructured=True,  start='202004', final='202103', start_date='2020-04-01', final_date='2021-03-31',
                                     data_frequency='variable', freq3d='PT6H', freq2d='PT3H',
                                     wind_vars=['u10'],
                                     syclops_file='SyCLoPS_classified_CASESM2-noc.parquet'),
                      icon    = dict(file='icon_d3hp003_1year_tracking/icon_d3hp003_hp8.etc_stitched_nodes.txt', cat_name='icon_d3hp003',
                                     unstructured=True,  start='202003', final='202102', start_date='2020-03-01', final_date='2021-02-28',
                                     data_frequency='variable', freq3d='PT6H', freq2d='PT6H',
                                     wind_vars=('uas', 'vas'),
                                     syclops_file='SyCLoPS_classified_ICON-d3hp003.parquet'),
                      nicam   = dict(file='nicam_gl11_tracking/nicam_gl11_hp8.etc_stitched_nodes.txt', cat_name='nicam_gl11',
                                     unstructured=True,  start='202003', final='202102', start_date='2020-03-01', final_date='2021-02-28',
                                     data_frequency='variable', freq3d='PT6H', freq2d='PT3H',
                                     wind_vars=('uas', 'vas'),
                                     syclops_file='SyCLoPS_classified_NICAM.parquet'),
                      scream  = dict(file='screamv2_ne120_tracking/screamv2_ne120_hp8.etc_stitched_nodes.txt', cat_name='scream_ne120_inst',
                                     unstructured=True,  start='201909', final='202008', start_date='2019-09-01', final_date='2020-08-31',
                                     data_frequency='default',
                                     wind_vars=None,
                                     syclops_file='SyCLoPS_classified_SCREAM_ne120.parquet'),
                      um      = dict(file='um_glm_n2560_RAL3p3_tracking/um_glm_n2560_RAL3p3_hp8.etc_stitched_nodes.txt', cat_name='um_glm_n2560_RAL3p3',
                                     unstructured=True,  start='202003', final='202102', start_date='2020-03-01', final_date='2021-02-28',
                                     data_frequency='variable', freq3d='PT3H', freq2d='PT1H',
                                     wind_vars=('uas', 'vas'),
                                     syclops_file='um_glm_n2560_RAL3p3.csv'),
                      conus404= dict(file='era5_tracking_full/era5.etc_stitched_nodes.txt', cat_name='wrf_conus',
                                     unstructured=False, start='202001', final='202012', start_date='2020-01-01', final_date='2020-12-31',
                                     data_frequency='default',
                                     wind_vars=('eastward_wind', 'northward_wind'),
                                     syclops_file='SyCLoPS_classified_ERA5.parquet'),
                 )

    current_location = "NERSC" #
    cat              = intake.open_catalog("https://digital-earths-global-hackathon.github.io/catalog/catalog.yaml")[current_location]
    gcd10            = sphere_distance(lon1=0., lat1=0., lon2=10., lat2=0.)
    bins             = np.arange(0, 151)
    zoom_level       = args.zoom_level
    
    # Determine which cases to process
    cases_to_process = [args.case] if args.case is not None else list(cases.keys())
    
    print(f"Processing cases: {cases_to_process}")
    print(f"Zoom level: {zoom_level}")
    
    for case in cases_to_process:
        output_file = f"{output_dir}ETC_histogram_{case}_zoom{zoom_level}.nc"
        if os.path.exists(output_file):
            print(f"\nOutput file {output_file} already exists. Skipping {case}.")
            continue
        # Skip era5 for now
        if case.lower() in ['era5', 'casesm2']:
            print(f"\nSkipping {case} (for now)")
            continue
        
        print(f"\nProcessing {case}...")
        storm_dfs = parse_storm_file(parent_dir + cases[case]['file'], unstructured_mesh=cases[case]['unstructured'])
        storm_dfs['year_month'] = storm_dfs['year'] * 100 + storm_dfs['month']

        filtered_df = storm_dfs[(storm_dfs['year_month'] >= int(cases[case]['start'])) 
                                & (storm_dfs['year_month'] <= int(cases[case]['final']))]

        catalog_params = dict()
        catalog_params['zoom'] = zoom_level
        if cases[case]['data_frequency'] == 'variable':
            catalog_params['time'] = cases[case]['freq2d']
        ds = cat[cases[case]['cat_name']](**catalog_params).to_dask()

        if case.lower() == 'era5':
            ds['cell'] = ds['cell'].astype(np.int64)
        ds = ds.pipe(egh.attach_coords)

        if case == 'nicam':
            print("Adjusting NICAM times by -1.5 hours...")
            ds['time'] = ds['time'] - pd.Timedelta(hours=1.5)

        if case == 'conus404':
            ds = ds.rename({'Time':'time'})
        
        # Compute sfcWind if wind_vars are specified
        if 'wind_vars' in cases[case] and cases[case]['wind_vars'] is not None:
            if len(cases[case]['wind_vars']) == 2:
                u_var, v_var = cases[case]['wind_vars']
                print(f"  Computing sfcWind from {u_var} and {v_var}")
                ds['sfcWind'] = np.sqrt(ds[u_var]**2 + ds[v_var]**2)
                ds['sfcWind'].attrs = {'long_name': 'Surface wind speed', 'units': 'm/s'}
            elif len(cases[case]['wind_vars']) == 1:
                print(f"  Renaming {cases[case]['wind_vars']} to sfcWind")
                ds = ds.rename({cases[case][wind_vars]:'sfcWind'})

        ds_histograms = compute_storm_wind_histograms_dask(
            df_tracks=filtered_df,
            ds=ds,
            sphere_distance=sphere_distance,
            gcd_threshold=gcd10,
            bins=bins,
            batch_size=50  # Adjust based on memory availability
        )

        # Save results
        print("\nSaving results...")
        ds_histograms.to_netcdf(output_file)
    
        print("\nDone! Summary:")
        print(f"  Storms processed: {len(ds_histograms.storm_id)}")
        print(f"  Max track length: {len(ds_histograms.track_time)}")
        print(f"  Valid observations: {ds_histograms.valid.sum().values}")
