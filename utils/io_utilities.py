#!/usr/bin/env python3

import os
import subprocess
import sys
import shutil
from utils.list_files_for_TE import generate_file_list, transform_file_list
import yaml
from concurrent.futures import ThreadPoolExecutor

def run_command(cmd, use_srun=False, num_procs=None, machine='perlmutter'):
    """Run a shell command, optionally using srun with specified number of processes."""
    # Convert cmd to strings if it's a list
    if isinstance(cmd, list):
        cmd = [str(item) for item in cmd]
        print(f"Running: {' '.join(cmd)}")
    else:
        print(f"Running: {cmd}")

    if num_procs is None:
        if (machine.lower()=='perlmutter') or (machine.lower()=='chrysalis'):
            num_procs=64
        elif machine.lower()=='compy':
            num_procs=40
    
    if use_srun:
        if isinstance(cmd, list):
            full_cmd = ["srun", "-n", str(num_procs)] + cmd
        else:
            full_cmd = ["srun", "-n", str(num_procs)] + cmd.split()
        result = subprocess.run(full_cmd, check=True, text=True, capture_output=True)
    else:
        result = subprocess.run(cmd, shell=isinstance(cmd, str), check=True, text=True, capture_output=True)
    
    print(result.stdout)
    sys.stdout.flush()
    return None

def safe_update(config, new_dict):
    """
    Update the configuration dictionary with new values, ensuring that existing keys are not overwritten.
    
    Args:
        config (dict): The original configuration dictionary
        new_dict (dict): The new values to add in the configuration
        
    Returns:
        dict: The updated configuration dictionary
    """
    config.update({k: v for k, v in new_dict.items() if k not in config})
    return config

def ensure_dir(directory):
    """Create directory if it doesn't exist."""
    if not os.path.exists(directory):
        os.makedirs(directory)
        print(f"Created directory: {directory}")

def load_yaml_file(file_path):
    """
    Load a YAML file and return its contents.
    
    Args:
        file_path (str): Path to the YAML file
        
    Returns:
        dict: The loaded YAML content as a dictionary
    """
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
        print(f"Loaded configuration from {file_path}")
    return data

def load_config_and_generate_files(config_file):
    """
    Load configuration from a YAML file and generate input file lists.
        
    Args:
        config_file (str): Path to the YAML configuration file
        
    Returns:
        dict: The loaded configuration dictionary
    """
    # Load configuration from YAML file
    input_config = load_yaml_file(config_file)
        
    # Generate file lists
    if os.path.exists(input_config['in_data_list']):
        os.remove(input_config['in_data_list'])

    # generate_file_list expects None type when no static file is present
    if input_config['static_file'] == "":
        input_config['static_file'] = None

    if input_config['in_data_list']:
        generate_file_list(input_config['patterns'], input_config['in_data_list'], 
                           input_config['pattern_match'], static_file=input_config['static_file'])
        
    return input_config

def transform_file_lists(config):
    """Transform input file lists to output file lists for each feature type."""
    if config['in_data']:
        return
    
    # TC files
    if config['tc_detected_nodes']:
        transform_file_list(config['in_data_list'], config['tc_detected_nodes'], config['pattern_match'], 
                            prefix=f"{config['output_dir']}TC_det_nodes_{config['shortname']}_", suffix=".txt")
    
    if config['tc_filtered_nodes_file']:
        transform_file_list(config['in_data_list'], config['tc_filtered_nodes_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}TC_filt_nodes_{config['shortname']}_", suffix=".nc")
    
    if config['tc_tracks_list']:
        transform_file_list(config['in_data_list'], config['tc_tracks_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}TC_tracks_{config['shortname']}_", suffix=".nc")
    
    # AR files
    if config['ar_detected_blobs_list']:
        transform_file_list(config['in_data_list'], config['ar_detected_blobs_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}AR_det_nodes_{config['shortname']}_", suffix=".nc")
    
    if config['ar_filtered_nodes_list']:
        transform_file_list(config['in_data_list'], config['ar_filtered_nodes_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}AR_filt_nodes_{config['shortname']}_", suffix=".nc")
    
    if config['ar_tracks_list']:
        transform_file_list(config['in_data_list'], config['ar_tracks_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}AR_tracks_{config['shortname']}_", suffix=".nc")
    
    # ETC files
    if config['etc_detected_nodes']:
        transform_file_list(config['in_data_list'], config['etc_detected_nodes'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_det_nodes_{config['shortname']}_", suffix=".txt")
    
    if config['etc_filtered_nodes_list']:
        transform_file_list(config['in_data_list'], config['etc_filtered_nodes_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_filt_nodes_{config['shortname']}_", suffix=".nc")
    
    if config['etc_tracks_list']:
        transform_file_list(config['in_data_list'], config['etc_tracks_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_tracks_{config['shortname']}_", suffix=".nc")

def process_file(file_name, config, drop_vars=["lon", "lat"]):
    # Check that the file exists
    if not os.path.exists(file_name):
        raise FileNotFoundError(f"File {file_name} does not exist.")
    else:
        print(f'Processing file: {file_name}')
    # Run the ncks command
    if config['do_append_crs']:
        subprocess.run(["ncks", "-A", config['crs_file'], file_name], check=True)
    
    # Run the unify_dimensions.py script
    if config['do_unify_dimensions']:
        if not drop_vars:
            run_command_list = ["python", "unify_dimensions.py", "--input_file", file_name]
        else:
            run_command_list = ["python", "unify_dimensions.py", "--input_file", file_name] + ["--drop_vars"] + drop_vars
        subprocess.run(run_command_list, check=True)

    # Adjust file permissions
    if config['do_open_permissions']:
        os.chmod(file_name, 0o644)

def process_file_list(file_list, config, drop_vars=["lon", "lat"], max_workers=64):
    # Run file processing in parallel across CPUs
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(process_file, file_name.strip(), config, drop_vars)
            for file_name in file_list
        ]
        for future in futures:
            future.result()  # Wait for completion and propagate exceptions if any

def read_and_process_file_list(file_list_path, config, drop_vars=["lon", "lat"], max_workers=64):
    with open(file_list_path, "r") as file_list_text:
        file_list = file_list_text.readlines()
        process_file_list(file_list, config, drop_vars=drop_vars, max_workers=max_workers)

def setup_env(machine='perlmutter'):
    """Set up the environment by sourcing necessary files if TempestExtremes executables aren't available."""
    # Check if DetectNodes is already in PATH
    if shutil.which("DetectNodes"):
        print("TempestExtremes executables already available in PATH. Skipping environment setup.")
        return
    
    # If not found, source the environment file
    print("TempestExtremes executables not found. Loading E3SM environment...")
    if machine.lower()=='perlmutter':
        source_cmd = "source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh"
    elif machine.lower()=='compy':
        source_cmd = "source /share/apps/E3SM/conda_envs/load_latest_e3sm_unified_compy.sh"
    elif machine.lower()=='chrysalis':
        source_cmd = "source /lcrc/soft/climate/e3sm-unified/load_latest_e3sm_unified_chrysalis.sh"
    subprocess.run(source_cmd, shell=True, executable="/bin/bash")
    
    # Verify that it worked
    if not shutil.which("DetectNodes"):
        print("WARNING: Failed to find DetectNodes even after loading environment. Check your configuration.")

def file_cleanup(config, drop_vars=['lon', 'lat']):
    if not config['do_file_cleanup']:
        print("\n----- Skipping File Cleanup -----\n")
        return

    print("\n----- Starting File Cleanup -----\n")

    def cleanup_tc():
        print("Cleaning up TC files...")
        if config['tc_detected_nodes']:
            os.chmod(config["tc_detected_nodes"], 0o644)
        if config['tc_stitched_nodes']:
            os.chmod(config["tc_stitched_nodes"], 0o644)
        if config['in_data'] or config['in']:
            if config['tc_filtered_nodes_file']:
                process_file(config['tc_filtered_nodes_file'], config, drop_vars=drop_vars)
            if config['tc_tracks_file']:
                process_file(config['tc_tracks_file'],         config, drop_vars=drop_vars)
        if config['in_data_list'] or config['in_list']:
            if config['tc_filtered_nodes_list']:
                read_and_process_file_list(config['tc_filtered_nodes_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
            if config['tc_tracks_list']:
                read_and_process_file_list(config['tc_tracks_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
        print("TC files cleaned up")

    def cleanup_ar():
        print("Cleaning up AR files...")
        if config['in_data'] or config['in']:
            if config['ar_detected_blobs_file']:
                process_file(config['ar_detected_blobs_file'], config, drop_vars=drop_vars)
            if config['ar_filtered_nodes_file']:
                process_file(config['ar_filtered_nodes_file'], config, drop_vars=drop_vars)
            if config['ar_tracks_file']:
                process_file(config['ar_tracks_file'],         config, drop_vars=drop_vars)
        if config['in_data_list'] or config['in_list']:
            if config['ar_detected_blobs_list']:
                read_and_process_file_list(config['ar_detected_blobs_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
            if config['ar_filtered_nodes_list']:
                read_and_process_file_list(config['ar_filtered_nodes_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
            if config['ar_tracks_list']:
                read_and_process_file_list(config['ar_tracks_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
        print("AR files cleaned up")

    def cleanup_etc():
        print("Cleaning up ETC files...")
        if config['etc_detected_nodes']:
            os.chmod(config["etc_detected_nodes"], 0o644)
        if config['etc_stitched_nodes']:
            os.chmod(config["etc_stitched_nodes"], 0o644)
        if config['in_data'] or config['in']:
            if config['etc_filtered_nodes_file']:
                process_file(config['etc_filtered_nodes_file'], config, drop_vars=drop_vars)
            if config['etc_tracks_file']:
                process_file(config['etc_tracks_file'],         config, drop_vars=drop_vars)
        if config['in_data_list'] or config['in_list']:
            if config['etc_filtered_nodes_list']:
                read_and_process_file_list(config['etc_filtered_nodes_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
            if config['etc_tracks_list']:
                read_and_process_file_list(config['etc_tracks_list'], config,
                                           drop_vars=drop_vars, max_workers=64)
        print("ETC files cleaned up")

    if config['do_detect_tc']:
        cleanup_tc()
    if config['do_detect_ar']:
        cleanup_ar()
    if config['do_detect_etc']:
        cleanup_etc()
