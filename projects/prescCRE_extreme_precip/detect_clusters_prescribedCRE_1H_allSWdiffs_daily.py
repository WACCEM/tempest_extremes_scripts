#!/usr/bin/env python3

import os
from utils.build_TE_commands import *
from utils.io_utilities import *


def detect_ar(config,
              config_DetectBlobs=None,
              config_NodeFileFilter=None,
              config_StitchBlobs=None,
              config_BlobStats=None):
    """Detect atmospheric rivers using TempestExtremes commands."""
    if not config['do_detect_ar']:
        print("\n----- Skipping AR Detection -----\n")
        return
    
    print("\n----- Starting Blob Detection -----\n")
    
    if not config['in_data_list']:
        # Clean up if using individual files
        for f in [config['ar_detected_blobs_file'], config['ar_filtered_nodes_file'], 
                  config['ar_tracks_file']]:
            if os.path.exists(f):
                os.remove(f)
    
    # DetectBlobs (Step 1)
    if config_DetectBlobs is not None:
        run_command(build_DetectBlobs_command(config_DetectBlobs), use_srun=True)
    
    # NodeFileFilter (Step 2)
    if config_NodeFileFilter is not None:
        run_command(build_NodeFileFilter_command(config_NodeFileFilter), use_srun=True)
    
    # StitchBlobs (Step 3)
    if config_StitchBlobs is not None:
        run_command(build_StitchBlobs_command(config_StitchBlobs), use_srun=True, num_procs=1)

    # BlobStats (Step 4)
    if config_BlobStats is not None:
        run_command(build_BlobStats_command(config_BlobStats), use_srun=True, num_procs=1)

    print("\n----- Blob Detection Complete -----\n")


def main():
    print("Starting main")
    
    # Setup environment
    setup_env()
    
    # Create configuration dictionary to store all paths and settings
    config = {}
    
    # Load configuration and generate file lists
    input_config  = load_config_and_generate_files("projects/prescCRE_extreme_precip/prescribedCRE_1H_allSWdiffs_io_config.yaml")
        
    # Update config with values from input_config
    config = safe_update(config, input_config)
    ensure_dir(config['output_dir'])

    print("Let's print some keys")
    for key in config.keys():
        print(f"{key}:", config[key])

    # Load the AR yaml files
    config_AR_DetectBlobs     = load_yaml_file('projects/prescCRE_extreme_precip/config_cluster_DetectBlobs.yaml')
    config_AR_StitchBlobs     = load_yaml_file('projects/prescCRE_extreme_precip/config_cluster_StitchBlobs.yaml')
    config_AR_BlobStats       = load_yaml_file('projects/prescCRE_extreme_precip/config_cluster_BlobStats.yaml')


    # Update AR config files with IO stuff 
    config_AR_DetectBlobs     = safe_update(config_AR_DetectBlobs,     config)
    config_AR_StitchBlobs     = safe_update(config_AR_StitchBlobs,     config)
    config_AR_BlobStats       = safe_update(config_AR_BlobStats,       config)
    
    # AR files
    config['ar_detected_blobs_file']  = f"{config['output_dir']}/{config['shortname']}.ar_detected_blobs.nc"
    config['ar_detected_blobs_list']  = f"{config['output_dir']}/{config['shortname']}.ar_detected_blobs.txt"
    config['ar_tracks_file']          = f"{config['output_dir']}/{config['shortname']}.ar_tracks.nc"
    config['ar_tracks_list']          = f"{config['output_dir']}/{config['shortname']}.ar_tracks.txt"
    config['ar_stats_file']           = f"{config['output_dir']}/{config['shortname']}.ar_stats.txt"


    # AR IO files
    if config_AR_DetectBlobs['in_data_list']:
        config_AR_DetectBlobs['out_list']          = config['ar_detected_blobs_list']
        config_AR_StitchBlobs['in_list']           = config['ar_filtered_nodes_list']
        config_AR_StitchBlobs['out_list']          = config['ar_tracks_list']
    if config_AR_DetectBlobs['in_data']:
        config_AR_DetectBlobs['out']               = config['ar_detected_blobs_file']
        config_AR_StitchBlobs['in']                = config['ar_filtered_nodes_file']
        config_AR_StitchBlobs['out']               = config['ar_tracks_file']
    config_AR_BlobStats['out']                     = config['ar_stats_file']


    # Transform file lists
    transform_file_lists(config)

    print('Tracking configs:')
    print(config_AR_DetectBlobs)
    print(config_AR_StitchBlobs)
    print(config_AR_BlobStats)
    
    # Feature detection flags
    config['do_detect_tc']    = False
    config['do_detect_ar']    = True
    config['do_detect_etc']   = False
    config['do_file_cleanup'] = False
    
    # Run detection
    detect_ar(config,
              config_DetectBlobs=config_AR_DetectBlobs,
              config_NodeFileFilter=None,
              config_StitchBlobs=config_AR_StitchBlobs,
              config_BlobStats=config_AR_BlobStats)
    
    file_cleanup(config, drop_vars=[])

if __name__ == "__main__":
    main()
