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
               config_VariableProcessor=None,
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

    if config_VariableProcessor is not None:
        run_command(build_VariableProcessor_command(config_VariableProcessor), use_srun=True)
    
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
    input_config  = load_config_and_generate_files("projects/kmscale_hackathon/era5_io_config.yaml")
        
    # Update config with values from input_config
    config = safe_update(config, input_config)

    print("Let's print some keys")
    for key in config.keys():
        print(f"{key}:", config[key])

    input_config2 = load_config_and_generate_files("projects/kmscale_hackathon/era5_io_config_pt2.yaml")
    config['input_pr_ws_list'] = input_config2['in_data_list']

    # Load the TC yaml files
    config_TC_DetectNodes     = load_yaml_file('projects/kmscale_hackathon/config_ERA5_TC_DetectNodes.yaml')
    config_TC_StitchNodes     = load_yaml_file('projects/kmscale_hackathon/config_TC_StitchNodes.yaml')
    config_TC_NodeFileFilter  = load_yaml_file('projects/kmscale_hackathon/config_ERA5_TC_NodeFileFilter.yaml')
    config_TC_StitchBlobs     = load_yaml_file('projects/kmscale_hackathon/config_ERA5_TC_StitchBlobs.yaml')

    # Load the AR yaml files
    config_AR_DetectBlobs     = load_yaml_file('projects/kmscale_hackathon/config_ERA5_AR_DetectBlobs.yaml')
    config_AR_NodeFileFilter  = load_yaml_file('projects/kmscale_hackathon/config_ERA5_AR_NodeFileFilter.yaml')
    config_AR_StitchBlobs     = load_yaml_file('projects/kmscale_hackathon/config_ERA5_AR_StitchBlobs.yaml')

    # Load the ETC yaml files
    config_ETC_DetectNodes       = load_yaml_file('projects/kmscale_hackathon/config_ERA5_ETC_DetectNodes.yaml')
    config_ETC_StitchNodes       = load_yaml_file('projects/kmscale_hackathon/config_ETC_StitchNodes.yaml')
    config_ETC_NodeFileFilter    = load_yaml_file('projects/kmscale_hackathon/config_ERA5_ETC_NodeFileFilter.yaml')
    config_ETC_VariableProcessor = load_yaml_file('projects/kmscale_hackathon/config_ERA5_CyclVort850_VariableProcessor.yaml')
    config_ETC_StitchBlobs       = load_yaml_file('projects/kmscale_hackathon/config_ERA5_ETC_StitchBlobs.yaml')

    # Update TC config files with IO stuff 
    config_TC_DetectNodes     = safe_update(config_TC_DetectNodes,     config)
    config_TC_StitchNodes     = safe_update(config_TC_StitchNodes,     config)
    config_TC_NodeFileFilter  = safe_update(config_TC_NodeFileFilter,  config)
    config_TC_StitchBlobs     = safe_update(config_TC_StitchBlobs,     config)

    # Update AR config files with IO stuff 
    config_AR_DetectBlobs     = safe_update(config_AR_DetectBlobs,     config)
    config_AR_NodeFileFilter  = safe_update(config_AR_NodeFileFilter,  config)
    config_AR_StitchBlobs     = safe_update(config_AR_StitchBlobs,     config)

    # Update ETC config files with IO stuff 
    config_ETC_DetectNodes       = safe_update(config_ETC_DetectNodes,       config)
    config_ETC_StitchNodes       = safe_update(config_ETC_StitchNodes,       config)
    config_ETC_NodeFileFilter    = safe_update(config_ETC_NodeFileFilter,    config)
    config_ETC_VariableProcessor = safe_update(config_ETC_VariableProcessor, config)
    config_ETC_StitchBlobs       = safe_update(config_ETC_StitchBlobs,       config)
    print("Let's print out the config_TC_DetectNodes keys after update")
    for key in config_TC_DetectNodes.keys():    
        print(f"{key}:", config_TC_DetectNodes[key])
    
    # TC files
    config['tc_detected_nodes']       = f"{config['output_dir']}/{config['shortname']}.tc_detected_nodes.txt"
    config['tc_stitched_nodes']       = f"{config['output_dir']}/{config['shortname']}.tc_stitched_nodes.txt"
    config['tc_filtered_nodes_file']  = f"{config['output_dir']}/{config['shortname']}.tc_filtered_nodes.nc"
    config['tc_filtered_nodes_list']  = f"{config['output_dir']}/{config['shortname']}.tc_filtered_nodes.txt"
    config['tc_tracks_file']          = f"{config['output_dir']}/{config['shortname']}.tc_tracks.nc"
    config['tc_tracks_list']          = f"{config['output_dir']}/{config['shortname']}.tc_tracks.txt"
    config['tc_climatology']          = f"{config['output_dir']}/{config['shortname']}.tc_climatology.nc"
    
    # AR files
    config['ar_detected_blobs_file']  = f"{config['output_dir']}/{config['shortname']}.ar_detected_blobs.nc"
    config['ar_detected_blobs_list']  = f"{config['output_dir']}/{config['shortname']}.ar_detected_blobs.txt"
    config['ar_filtered_nodes_file']  = f"{config['output_dir']}/{config['shortname']}.ar_filtered_nodes.nc"
    config['ar_filtered_nodes_list']  = f"{config['output_dir']}/{config['shortname']}.ar_filtered_nodes.txt"
    config['ar_tracks_file']          = f"{config['output_dir']}/{config['shortname']}.ar_tracks.nc"
    config['ar_tracks_list']          = f"{config['output_dir']}/{config['shortname']}.ar_tracks.txt"
    
    # ETC files
    config['etc_detected_nodes']      = f"{config['output_dir']}/{config['shortname']}.etc_detected_nodes.txt"
    config['etc_stitched_nodes']      = f"{config['output_dir']}/{config['shortname']}.etc_stitched_nodes.txt"
    config['etc_cyclvort850_list']    = f"{config['output_dir']}/{config['shortname']}.etc_cyclvort850_list.txt"
    config['etc_cyclvort850_file']    = f"{config['output_dir']}/{config['shortname']}.etc_cyclvort850_file.nc"
    config['etc_filtered_nodes_file'] = f"{config['output_dir']}/{config['shortname']}.etc_filtered_nodes.nc"
    config['etc_filtered_nodes_list'] = f"{config['output_dir']}/{config['shortname']}.etc_filtered_nodes.txt"
    config['etc_tracks_file']         = f"{config['output_dir']}/{config['shortname']}.etc_tracks.nc"
    config['etc_tracks_list']         = f"{config['output_dir']}/{config['shortname']}.etc_tracks.txt"

    # First TC IO files
    if config_TC_DetectNodes['in_data_list']:
        config_TC_DetectNodes['out_file_list']     = config['tc_detected_nodes']
        config_TC_StitchNodes['in_list']           = config['tc_detected_nodes']
        config_TC_NodeFileFilter['in_data_list']   = config['input_pr_ws_list']
        config_TC_NodeFileFilter['out_data_list']  = config['tc_filtered_nodes_list']
        config_TC_StitchBlobs['in_list']           = config['tc_filtered_nodes_list']
        config_TC_StitchBlobs['out_list']          = config['tc_tracks_list']
    if config_TC_DetectNodes['in_data']:
        config_TC_DetectNodes['out']               = config['tc_detected_nodes']
        config_TC_StitchNodes['in']                = config['tc_detected_nodes']
        config_TC_NodeFileFilter['in_data']        = config['in_data']
        config_TC_NodeFileFilter['out_data']       = config['tc_filtered_nodes_file']
        config_TC_StitchBlobs['in']                = config['tc_filtered_nodes_file']
        config_TC_StitchBlobs['out']               = config['tc_tracks_file']
    config_TC_StitchNodes['out']                   = config['tc_stitched_nodes']
    config_TC_NodeFileFilter['in_nodefile']        = config['tc_stitched_nodes']

    # Next AR IO files
    if config_AR_DetectBlobs['in_data_list']:
        config_AR_DetectBlobs['out_list']          = config['ar_detected_blobs_list']
        config_AR_NodeFileFilter['in_data_list']   = config['ar_detected_blobs_list']
        config_AR_NodeFileFilter['out_data_list']  = config['ar_filtered_nodes_list']
        config_AR_StitchBlobs['in_list']           = config['ar_filtered_nodes_list']
        config_AR_StitchBlobs['out_list']          = config['ar_tracks_list']
    if config_AR_DetectBlobs['in_data']:
        config_AR_DetectBlobs['out']               = config['ar_detected_blobs_file']
        config_AR_NodeFileFilter['in_data']        = config['ar_detected_blobs_file']
        config_AR_NodeFileFilter['out_data']       = config['ar_filtered_nodes_file']
        config_AR_StitchBlobs['in']                = config['ar_filtered_nodes_file']
        config_AR_StitchBlobs['out']               = config['ar_tracks_file']
    config_AR_NodeFileFilter['in_nodefile']        = config['tc_stitched_nodes']

    # Finally ETC IO files
    if config_ETC_DetectNodes['in_data_list']:
        config_ETC_DetectNodes['out_file_list']       = config['etc_detected_nodes']
        config_ETC_StitchNodes['in_list']             = config['etc_detected_nodes']
        config_ETC_VariableProcessor['out_data_list'] = config['etc_cyclvort850_list']
        config_ETC_NodeFileFilter['in_data_list']     = config['etc_cyclvort850_list']
        config_ETC_NodeFileFilter['out_data_list']    = config['etc_filtered_nodes_list']
        config_ETC_StitchBlobs['in_list']             = config['etc_filtered_nodes_list']
        config_ETC_StitchBlobs['out_list']            = config['etc_tracks_list']
    if config_ETC_DetectNodes['in_data']:
        config_ETC_DetectNodes['out']                 = config['etc_detected_nodes']
        config_ETC_StitchNodes['in']                  = config['etc_detected_nodes']
        config_ETC_VariableProcessor['out_data']      = config['etc_cyclvort850_file']
        config_ETC_NodeFileFilter['in_data']          = config['etc_cyclvort850_file']
        config_ETC_NodeFileFilter['out_data']         = config['etc_filtered_nodes_file']
        config_ETC_StitchBlobs['in']                  = config['etc_filtered_nodes_file']
        config_ETC_StitchBlobs['out']                 = config['etc_tracks_file']
    config_ETC_StitchNodes['out']                     = config['etc_stitched_nodes']
    config_ETC_NodeFileFilter['in_nodefile']          = config['etc_stitched_nodes']


    # Transform file lists
    transform_file_lists(config)


    config_TC_DetectNodes['in_data'] = config['in_data']

    print('TC config:')
    print(config_TC_DetectNodes)
    print(config_TC_StitchNodes)
    print(config_TC_NodeFileFilter)
    print(config_TC_StitchBlobs)
    print('AR config:')
    print(config_AR_DetectBlobs)
    print(config_AR_NodeFileFilter)
    print(config_AR_StitchBlobs)
    print('ETC config:')
    print(config_ETC_DetectNodes)
    print(config_ETC_StitchNodes)
    print(config_ETC_VariableProcessor)
    print(config_ETC_NodeFileFilter)
    print(config_ETC_StitchBlobs)
    
    # Feature detection flags
    config['do_detect_tc']    = False
    config['do_detect_ar']    = False
    config['do_detect_etc']   = True
    config['do_file_cleanup'] = True
    
    # Run detection
    detect_tc(config, 
              config_DetectNodes=config_TC_DetectNodes,
              config_StitchNodes=config_TC_StitchNodes,
              config_NodeFileFilter=config_TC_NodeFileFilter,
              config_StitchBlobs=config_TC_StitchBlobs)
    detect_ar(config,
              config_DetectBlobs=config_AR_DetectBlobs,
              config_NodeFileFilter=config_AR_NodeFileFilter,
              config_StitchBlobs=config_AR_StitchBlobs)
    detect_etc(config,
               config_DetectNodes=config_ETC_DetectNodes,
               config_StitchNodes=config_ETC_StitchNodes,
               config_VariableProcessor=config_ETC_VariableProcessor,
               config_NodeFileFilter=config_ETC_NodeFileFilter,
               config_StitchBlobs=config_ETC_StitchBlobs)
    
    file_cleanup(config, drop_vars=[])

if __name__ == "__main__":
    main()
