#!/usr/bin/env python3

import os
from utils.build_TE_commands import *
from utils.io_utilities import *

def detect_tc(config,
              config_DetectNodes=None,
              config_StitchNodes=None,
              config_NodeFileFilter=None,
              config_StitchBlobs=None):
    """Detect tropical cyclones using TempestExtremes commands."""
    if not config['do_detect_tc']:
        print("\n----- Skipping TC Detection -----\n")
        return
    
    print("\n----- Starting TC Detection -----\n")
    
    # Clear out the TC files from previous attempts when using just a file
    if os.path.exists(config['tc_stitched_nodes']):
        os.remove(config['tc_stitched_nodes'])
    
    if not config['in_data_list']:
        # Clean up if using individual files
        for f in [config['tc_detected_nodes'], config['tc_filtered_nodes_file'], 
                  config['tc_tracks_file'], config['tc_climatology']]:
            if os.path.exists(f):
                os.remove(f)
    
    # DetectNodes (Step 1)
    if config_DetectNodes is not None:
        run_command(build_DetectNodes_command(config_DetectNodes), use_srun=True)
    
    # StitchNodes (Step 2)
    if config_StitchNodes is not None:
        run_command(build_StitchNodes_command(config_StitchNodes), use_srun=False)

     # NodeFileFilter (Step 3)
    if config_NodeFileFilter is not None:
        run_command(build_NodeFileFilter_command(config_NodeFileFilter), use_srun=True)
    
    # StitchBlobs (Step 4)
    if config_StitchBlobs is not None:
        run_command(build_StitchBlobs_command(config_StitchBlobs), use_srun=True, num_procs=1)
        # run_command(build_StitchBlobs_command(config_StitchBlobs), use_srun=True)

    print("\n----- TC Detection Complete -----\n")

def detect_ar(config,
              config_DetectBlobs=None,
              config_NodeFileFilter=None,
              config_StitchBlobs=None):
    """Detect atmospheric rivers using TempestExtremes commands."""
    if not config['do_detect_ar']:
        print("\n----- Skipping AR Detection -----\n")
        return
    
    print("\n----- Starting AR Detection -----\n")
    
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

    print("\n----- AR Detection Complete -----\n")

def detect_etc(config,
               config_DetectNodes=None,
               config_StitchNodes=None,
               config_NodeFileFilter=None,
               config_StitchBlobs=None):
    """Detect extratropical cyclones using TempestExtremes commands."""
    if not config['do_detect_etc']:
        print("\n----- Skipping ETC Detection -----\n")
        return
    
    print("\n----- Starting ETC Detection -----\n")
    
    if os.path.exists(config['etc_stitched_nodes']):
        os.remove(config['etc_stitched_nodes'])
        
    if not config['in_data_list']:
        # Clean up if using individual files
        for f in [config['etc_detected_nodes'], config['etc_filtered_nodes_file'], 
                  config['etc_tracks_file']]:
            if os.path.exists(f):
                os.remove(f)
    
    # DetectNodes (Step 1)
    if config_DetectNodes is not None:
        run_command(build_DetectNodes_command(config_DetectNodes), use_srun=True)
    
    # StitchNodes (Step 2)
    if config_StitchNodes is not None:
        run_command(build_StitchNodes_command(config_StitchNodes), use_srun=False)
    
    # NodeFileFilter (Step 3)
    if config_NodeFileFilter is not None:
        run_command(build_NodeFileFilter_command(config_NodeFileFilter), use_srun=True)
    
    # StitchBlobs (Step 4)
    if config_StitchBlobs is not None:
        run_command(build_StitchBlobs_command(config_StitchBlobs), use_srun=True, num_procs=1)

    print("\n----- ETC Detection Complete -----\n")

def main():
    print("Starting main")
    
    # Setup environment
    setup_env()
    
    # Create configuration dictionary to store all paths and settings
    config = {}
    
    # Load configuration and generate file lists
    input_config  = load_config_and_generate_files("projects/etc_extras_kmscale_hackathon/icon_io_config.yaml")
        
    # Update config with values from input_config
    config = safe_update(config, input_config)
    ensure_dir(config['output_dir'])

    print("Let's print some keys")
    for key in config.keys():
        print(f"{key}:", config[key])

    input_config2 = load_config_and_generate_files("projects/etc_extras_kmscale_hackathon/icon_io_config.yaml")
    config['input_pr_ws_list'] = input_config2['in_data_list']


    # Load the ETC yaml files
    config_ETC_DetectNodes    = load_yaml_file('projects/etc_extras_kmscale_hackathon/config_ETC_DetectNodes.yaml')
    config_ETC_StitchNodes    = load_yaml_file('projects/etc_extras_kmscale_hackathon/config_ETC_StitchNodes.yaml')
    #config_ETC_NodeFileFilter = load_yaml_file('projects/etc_extras_kmscale_hackathon/config_ETC_NodeFileFilter.yaml')
    #config_ETC_StitchBlobs    = load_yaml_file('projects/etc_extras_kmscale_hackathon/config_ETC_StitchBlobs.yaml')


    # Update ETC config files with IO stuff 
    config_ETC_DetectNodes    = safe_update(config_ETC_DetectNodes,    config)
    config_ETC_StitchNodes    = safe_update(config_ETC_StitchNodes,    config)
    #config_ETC_NodeFileFilter = safe_update(config_ETC_NodeFileFilter, config)
    #config_ETC_StitchBlobs    = safe_update(config_ETC_StitchBlobs,    config)

    # ETC files
    config['etc_detected_nodes']      = f"{config['output_dir']}/{config['shortname']}.etc_detected_nodes.txt"
    config['etc_stitched_nodes']      = f"{config['output_dir']}/{config['shortname']}.etc_stitched_nodes.txt"
    config['etc_filtered_nodes_file'] = f"{config['output_dir']}/{config['shortname']}.etc_filtered_nodes.nc"
    config['etc_filtered_nodes_list'] = f"{config['output_dir']}/{config['shortname']}.etc_filtered_nodes.txt"
    config['etc_tracks_file']         = f"{config['output_dir']}/{config['shortname']}.etc_tracks.nc"
    config['etc_tracks_list']         = f"{config['output_dir']}/{config['shortname']}.etc_tracks.txt"

    # Finally ETC IO files
    if config_ETC_DetectNodes['in_data_list']:
        config_ETC_DetectNodes['out_file_list']    = config['etc_detected_nodes']
        config_ETC_StitchNodes['in_list']          = config['etc_detected_nodes']
        #config_ETC_NodeFileFilter['in_data_list']  = config['input_pr_ws_list']
        #config_ETC_NodeFileFilter['out_data_list'] = config['etc_filtered_nodes_list']
        #config_ETC_StitchBlobs['in_list']          = config['etc_filtered_nodes_list']
        #config_ETC_StitchBlobs['out_list']         = config['etc_tracks_list']
    if config_ETC_DetectNodes['in_data']:
        config_ETC_DetectNodes['out']              = config['etc_detected_nodes']
        config_ETC_StitchNodes['in']               = config['etc_detected_nodes']
        #config_ETC_NodeFileFilter['in_data']       = config['in_data']
        #config_ETC_NodeFileFilter['out_data']      = config['etc_filtered_nodes_file']
        #config_ETC_StitchBlobs['in']               = config['etc_filtered_nodes_file']
        #config_ETC_StitchBlobs['out']              = config['etc_tracks_file']
    config_ETC_StitchNodes['out']                  = config['etc_stitched_nodes']
    #config_ETC_NodeFileFilter['in_nodefile']       = config['etc_stitched_nodes']


    # Transform file lists
    transform_file_lists(config)

    #print('TC config:')
    #print(config_TC_DetectNodes)
    #print(config_TC_StitchNodes)
    #print(config_TC_NodeFileFilter)
    #print(config_TC_StitchBlobs)
    #print('AR config:')
    #print(config_AR_DetectBlobs)
    #print(config_AR_NodeFileFilter)
    #print(config_AR_StitchBlobs)
    print('ETC config:')
    print(config_ETC_DetectNodes)
    print(config_ETC_StitchNodes)
    #print(config_ETC_NodeFileFilter)
    #print(config_ETC_StitchBlobs)
    
    # Feature detection flags
    config['do_detect_tc']    = False
    config['do_detect_ar']    = False
    config['do_detect_etc']   = True
    config['do_file_cleanup'] = True
    
    # Run detection
    detect_tc(config, 
              config_DetectNodes=None,
              config_StitchNodes=None,
              config_NodeFileFilter=None,
              config_StitchBlobs=None)
    detect_ar(config,
              config_DetectBlobs=None,
              config_NodeFileFilter=None,
              config_StitchBlobs=None)
    detect_etc(config,
               config_DetectNodes=config_ETC_DetectNodes,
               config_StitchNodes=config_ETC_StitchNodes,
               config_NodeFileFilter=None,
               config_StitchBlobs=None)
    
    file_cleanup(config, drop_vars=[])

if __name__ == "__main__":
    main()
