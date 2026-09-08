#!/usr/bin/env python3

import os
import subprocess
import sys
import shutil
from utils.list_files_for_TE import generate_file_list, transform_file_list, merge_file_lists
import utils.build_TE_commands as build_TE_commands
import yaml
from concurrent.futures import ThreadPoolExecutor

# Recipe keys that configure a step rather than name a TempestExtremes argument
RECIPE_META_SUFFIXES = ('config', 'srun', 'num_procs')

# Token that refers back to the top-level input file list
IN_DATA_LIST_TOKEN = 'in_data_list'

# Token that refers to the top-level time-invariant file
STATIC_FILE_TOKEN = 'static_file'

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
    
    # Make sure output directory exists if it is specified
    if 'output_dir' in input_config:
        ensure_dir(input_config['output_dir'])
        
    # Generate file lists
    if os.path.exists(input_config['in_data_list']):
        os.remove(input_config['in_data_list'])

    # generate_file_list expects None type when no static file is present
    if input_config['static_file'] == "":
        input_config['static_file'] = None


    if input_config['in_data_list']:
        # generate_file_list(input_config['patterns'], input_config['in_data_list'], 
        #                    input_config['pattern_match'], static_file=input_config['static_file'],
        #                    era5_start_month=era5_start_month, era5_final_month=era5_final_month)
        generate_file_list(input_config['in_data_list'], input_config)
        
    return input_config

def transform_file_lists(config):
    """Transform input file lists to output file lists for each feature type."""
    if config['in_data']:
        return
    
    # TC files
    if 'tc_detected_nodes' in config:
        transform_file_list(config['in_data_list'], config['tc_detected_nodes'], config['pattern_match'], 
                            prefix=f"{config['output_dir']}TC_det_nodes_{config['shortname']}_", suffix=".txt")
    
    if 'tc_filtered_nodes_file' in config:
        transform_file_list(config['in_data_list'], config['tc_filtered_nodes_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}TC_filt_nodes_{config['shortname']}_", suffix=".nc")
    
    if 'tc_tracks_list' in config:
        transform_file_list(config['in_data_list'], config['tc_tracks_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}TC_tracks_{config['shortname']}_", suffix=".nc")
    
    # AR files
    if 'ar_detected_blobs_list' in config:
        transform_file_list(config['in_data_list'], config['ar_detected_blobs_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}AR_det_nodes_{config['shortname']}_", suffix=".nc")
    
    if 'ar_filtered_nodes_list' in config:
        transform_file_list(config['in_data_list'], config['ar_filtered_nodes_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}AR_filt_nodes_{config['shortname']}_", suffix=".nc")
    
    if 'ar_tracks_list' in config:
        transform_file_list(config['in_data_list'], config['ar_tracks_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}AR_tracks_{config['shortname']}_", suffix=".nc")
    
    # ETC files
    if 'etc_detected_nodes' in config:
        transform_file_list(config['in_data_list'], config['etc_detected_nodes'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_det_nodes_{config['shortname']}_", suffix=".txt")
    
    if 'etc_filtered_nodes_list' in config:
        transform_file_list(config['in_data_list'], config['etc_filtered_nodes_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_filt_nodes_{config['shortname']}_", suffix=".nc")
        
    if 'etc_cyclvort850_list' in config:
        transform_file_list(config['in_data_list'], config['etc_cyclvort850_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_cyclvort850_{config['shortname']}_", suffix=".nc")

    if 'etc_tracks_list' in config:
        transform_file_list(config['in_data_list'], config['etc_tracks_list'], config['pattern_match'],
                            prefix=f"{config['output_dir']}ETC_tracks_{config['shortname']}_", suffix=".nc")

    # New files naming convention (need to clean this up in the future)
    for file_out_list in ['tc_detectnodes_out', 'tc_stitchnodes_out', 'tc_nodefilefilter_out',
                          'ar_detectblobs_out', 'ar_variableprocessor_out', 'ar_nodefilefilter_out',
                          'ar_stitchblobs_out', 'ar_blobstats_out',
                          'etc_detectnodes_out', 'etc_stitchnodes_out', 'etc_nodefilefilter_out']:
        if file_out_list in config:
            transform_file_list(config['in_data_list'], config[file_out_list], config['pattern_match'],
                                prefix=f"{config['output_dir']}{file_out_list[-3:]}{config['shortname']}_", suffix=".txt")

def _is_list_arg(arg):
    """A TempestExtremes argument names a file list when it ends in _list."""
    return arg == 'in_list' or arg.endswith('_list')

def static_file_path(token, config):
    """
    Return the path for a time-invariant file token, or None if not one.

    `static_file` is reserved for the top-level key of that name; any other name
    is looked up in the optional `static_files` mapping.

    Args:
        token (str): The recipe token
        config (dict): The loaded io configuration

    Returns:
        str or None: The resolved path, or None if the token is not static
    """
    if token == STATIC_FILE_TOKEN:
        return config.get('static_file') or None
    return (config.get('static_files') or {}).get(token)

def parse_recipe(config):
    """
    Parse the processing recipes declared in an io config.

    A recipe is declared with a `{FEATURE}_steps` key holding a semicolon
    separated, ordered list of TempestExtremes binaries. Each step then declares
    its file arguments with `{FEATURE}_{Binary}_{te_arg}` keys, so the TE flag
    name is stated explicitly rather than inferred.

    Args:
        config (dict): The loaded io configuration

    Returns:
        dict: {feature: [(binary, {te_arg: token_string}), ...]} in declaration order
    """
    recipes = {}
    for key, value in config.items():
        if not key.endswith('_steps') or not value:
            continue
        feature = key[:-len('_steps')]
        steps = []
        for binary in [s.strip() for s in str(value).split(';') if s.strip()]:
            prefix = f"{feature}_{binary}_"
            args = {}
            for arg_key, arg_value in config.items():
                if not arg_key.startswith(prefix) or not arg_value:
                    continue
                arg = arg_key[len(prefix):]
                if arg in RECIPE_META_SUFFIXES:
                    continue
                args[arg] = arg_value
            steps.append((binary, args))
        recipes[feature] = steps
    return recipes

def resolve_token(token, config, is_list_arg):
    """
    Resolve a recipe token to a concrete path.

    `in_data_list` is a reserved shorthand for the top-level input file list,
    and `static_file` plus any name in `static_files` resolve to a single
    time-invariant file. Any other token names a file produced by an earlier
    step: for a list argument it resolves to the list file holding one entry
    per input file, otherwise to a single output file.

    Args:
        token (str): The recipe token, e.g. 'ar_detectblobs_out.nc'
        config (dict): The loaded io configuration
        is_list_arg (bool): Whether the token is bound to a file-list argument

    Returns:
        str: The resolved absolute path
    """
    if token == IN_DATA_LIST_TOKEN:
        return config['in_data_list']

    static_path = static_file_path(token, config)
    if static_path:
        return static_path

    stem, _ = os.path.splitext(token)
    name = f"{config['shortname']}.{stem}.txt" if is_list_arg else f"{config['shortname']}.{token}"
    return os.path.join(config['output_dir'], name)

def materialize_recipe_lists(config):
    """
    Build every file list required by the recipes in `config`.

    Walks the recipes in declaration order. Output list tokens are expanded into
    one entry per input file with `transform_file_list`; input arguments naming
    several tokens are combined line-by-line with `merge_file_lists` so that
    TempestExtremes receives semicolon separated inputs.

    Args:
        config (dict): The loaded io configuration

    Returns:
        tuple: (resolved, artifacts) where `resolved` is
            {feature: [(binary, {te_arg: path}), ...]} and `artifacts` records the
            generated lists and single-file outputs for clobbering.
    """
    recipes = parse_recipe(config)
    registry = {}
    artifacts = {'entry_lists': [], 'merged_lists': [], 'single_outputs': []}
    resolved = {}

    for feature, steps in recipes.items():
        resolved_steps = []
        for binary, args in steps:
            resolved_args = {}
            for arg, token_string in args.items():
                tokens = [t.strip() for t in str(token_string).split(';') if t.strip()]
                if not tokens:
                    continue

                if not _is_list_arg(arg):
                    if len(tokens) > 1:
                        raise ValueError(
                            f"{feature}_{binary}_{arg} names {len(tokens)} tokens but "
                            f"'{arg}' is a single-file argument."
                        )
                    path = resolve_token(tokens[0], config, False)
                    if (arg.startswith('out') and tokens[0] != IN_DATA_LIST_TOKEN
                            and not static_file_path(tokens[0], config)):
                        artifacts['single_outputs'].append(path)
                    resolved_args[arg] = path
                    continue

                paths = []
                sources = []
                for token in tokens:
                    path = resolve_token(token, config, True)
                    if static_file_path(token, config):
                        sources.append(('literal', path))
                        paths.append(path)
                        continue
                    if token != IN_DATA_LIST_TOKEN and token not in registry:
                        stem, ext = os.path.splitext(token)
                        transform_file_list(
                            config['in_data_list'], path, config['pattern_match'],
                            prefix=f"{config['output_dir']}{config['shortname']}.{stem}_",
                            suffix=ext,
                        )
                        registry[token] = path
                        artifacts['entry_lists'].append(path)
                    sources.append(('list', path))
                    paths.append(path)

                if len(sources) == 1 and sources[0][0] == 'list':
                    resolved_args[arg] = paths[0]
                else:
                    merged = os.path.join(
                        config['output_dir'], f"{config['shortname']}.{feature}_{binary}_{arg}.txt"
                    )
                    merge_file_lists(sources, merged)
                    artifacts['merged_lists'].append(merged)
                    resolved_args[arg] = merged

            resolved_steps.append((binary, resolved_args))
        resolved[feature] = resolved_steps

    return resolved, artifacts

def clobber_recipe_outputs(config, artifacts):
    """
    Delete the files the recipes are about to produce, for a clean rerun.

    Only files named inside generated output lists and single-file outputs are
    removed; the input data referenced by merged input lists is never touched.

    Args:
        config (dict): The loaded io configuration
        artifacts (dict): The artifact record from `materialize_recipe_lists`
    """
    if not config.get('do_clobber', False):
        return

    stale = list(artifacts['single_outputs'])
    for list_path in artifacts['entry_lists']:
        if not os.path.exists(list_path):
            continue
        with open(list_path, 'r') as f:
            for line in f:
                stale.extend(entry for entry in line.strip().split(';') if entry)

    removed = 0
    for path in stale:
        if os.path.exists(path):
            os.remove(path)
            removed += 1
    print(f"Clobbered {removed} existing output files.")

def run_recipe(config, feature, resolved_steps, dry_run=False):
    """
    Run every step of one feature's recipe in order.

    Args:
        config (dict): The loaded io configuration
        feature (str): The recipe name, e.g. 'TC'
        resolved_steps (list): [(binary, {te_arg: path}), ...] from `materialize_recipe_lists`
        dry_run (bool): Print the assembled commands instead of running them
    """
    if not config.get(f'do_detect_{feature.lower()}', True):
        print(f"\n----- Skipping {feature} -----\n")
        return

    print(f"\n----- Starting {feature} -----\n")
    for binary, resolved_args in resolved_steps:
        step_config = {}
        step_config_file = config.get(f'{feature}_{binary}_config')
        if step_config_file:
            step_config = load_yaml_file(step_config_file)
        step_config = safe_update(dict(step_config), config)
        step_config.update(resolved_args)

        cmd = getattr(build_TE_commands, f'build_{binary}_command')(step_config)
        if dry_run:
            print(f"[dry run] {' '.join(str(item) for item in cmd)}")
            continue
        run_command(cmd,
                    use_srun=config.get(f'{feature}_{binary}_srun', True),
                    num_procs=config.get(f'{feature}_{binary}_num_procs', None))

    print(f"\n----- {feature} Complete -----\n")

def run_recipes(config, dry_run=False):
    """
    Materialize every recipe file list, optionally clobber, then run all recipes.

    Args:
        config (dict): The loaded io configuration
        dry_run (bool): Print the assembled commands instead of running them

    Returns:
        tuple: (resolved, artifacts) from `materialize_recipe_lists`
    """
    resolved, artifacts = materialize_recipe_lists(config)
    clobber_recipe_outputs(config, artifacts)
    for feature, resolved_steps in resolved.items():
        run_recipe(config, feature, resolved_steps, dry_run=dry_run)
    return resolved, artifacts

def file_cleanup_recipe(config, artifacts, drop_vars=['lon', 'lat'], max_workers=64):
    """
    Post-process the files produced by the recipes.

    NetCDF outputs go through `process_file_list`; text outputs only get their
    permissions opened up.

    Args:
        config (dict): The loaded io configuration
        artifacts (dict): The artifact record from `materialize_recipe_lists`
        drop_vars (list): Variables to drop when unifying dimensions
        max_workers (int): Thread pool size for parallel processing
    """
    if not config.get('do_file_cleanup', False):
        print("\n----- Skipping File Cleanup -----\n")
        return

    print("\n----- Starting File Cleanup -----\n")
    outputs = list(artifacts['single_outputs'])
    for list_path in artifacts['entry_lists']:
        if not os.path.exists(list_path):
            continue
        with open(list_path, 'r') as f:
            outputs.extend(line.strip() for line in f if line.strip())

    netcdf_files = [path for path in outputs
                    if path.endswith('.nc') and os.path.exists(path)]
    process_file_list(netcdf_files, config, drop_vars=drop_vars, max_workers=max_workers)

    if config.get('do_open_permissions', False):
        for path in outputs:
            if not path.endswith('.nc') and os.path.exists(path):
                os.chmod(path, 0o644)

    print("\n----- File Cleanup Complete -----\n")

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
