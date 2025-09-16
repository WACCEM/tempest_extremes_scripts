#!/usr/bin/env python3

def build_VariableProcessor_command(config):
    """Build the VariableProcessor command based on the provided configuration."""
    cmd = [
        "VariableProcessor",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out_data",            f"{config['out_data']}",
        "--out_file_list",       f"{config['out_file_list']}",
        "--preserve",            f"{config['preserve']}",
        "--var",                 f"{config['var']}",
        "--varout",              f"{config['varout']}",
        "--timefilter",          f"{config['timefilter']}",
        "--fillvalue",           f"{config['fillvalue']}",
        "--lonname",             f"{config['lonname']}",
        "--latname",             f"{config['latname']}",
        "--logdir",              f"{config['logdir']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    return cmd

def build_DetectNodes_command(config):
    """Build the DetectNodes command based on the provided configuration."""
    cmd = [
        "DetectNodes",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out",                 f"{config['out']}",
        "--out_file_list",       f"{config['out_file_list']}",
        "--searchbymin",         f"{config['searchbymin']}",
        "--searchbymax",         f"{config['searchbymax']}",
        "--minlon",              f"{config['minlon']}",  
        "--maxlon",              f"{config['maxlon']}",
        "--minlat",              f"{config['minlat']}",
        "--maxlat",              f"{config['maxlat']}",
        "--minabslat",           f"{config['minabslat']}",  
        "--mergedist",           f"{config['mergedist']}",
        "--closedcontourcmd",    f"{config['closedcontourcmd']}",
        "--noclosedcontourcmd",  f"{config['noclosedcontourcmd']}",
        "--thresholdcmd",        f"{config['thresholdcmd']}",
        "--outputcmd",           f"{config['outputcmd']}",
        "--timefilter",          f"{config['timefilter']}",
        "--verbosity",           f"{config['verbosity']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    if config['out_header']:
        cmd.append("--out_header")
    return cmd

def build_StitchNodes_command(config):
    """Build the StitchNodes command based on the provided configuration."""
    cmd = [
        "StitchNodes",
        "--in",                  f"{config['in']}",
        "--in_list",             f"{config['in_list']}",
        "--out",                 f"{config['out']}",
        "--in_connect",          f"{config['in_connect']}",
        "--in_fmt",              f"{config['in_fmt']}",
        "--range",               f"{config['range']}",
        "--mintime",             f"{config['mintime']}",
        "--min_endpoint_dist",   f"{config['min_endpoint_dist']}",
        "--min_path_dist",       f"{config['min_path_dist']}",
        "--maxgap",              f"{config['maxgap']}",
        "--threshold",           f"{config['threshold']}",
        "--out_file_format",     f"{config['out_file_format']}",
    ]
    return cmd

def build_NodeFileEditor_command(config):
    """Build the NodeFileEditor command based on the provided configuration."""
    cmd = [
        "NodeFileEditor",
        "--in_nodefile",         f"{config['in_nodefile']}",
        "--in_nodefile_type",    f"{config['in_nodefile_type']}",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--in_fmt",              f"{config['in_fmt']}",
        "--out_fmt",             f"{config['out_fmt']}",
        "--out_nodefile",        f"{config['out_nodefile']}",
        "--out_nodefile_format", f"{config['out_nodefile_format']}",
        "--timefilter",          f"{config['timefilter']}",
        "--colfilter",           f"{config['colfilter']}",
        "--calculate",           f"{config['calculate']}",
        "--lonname",             f"{config['lonname']}",
        "--latname",             f"{config['latname']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    return cmd

def build_NodeFileFilter_command(config):
    """Build the NodeFileFilter command based on the provided configuration."""
    cmd = [
        "NodeFileFilter",
        "--in_nodefile",         f"{config['in_nodefile']}",
        "--in_nodefile_type",    f"{config['in_nodefile_type']}",
        "--in_fmt",              f"{config['in_fmt']}",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out_data",            f"{config['out_data']}",
        "--out_data_list",       f"{config['out_data_list']}",
        "--var",                 f"{config['var']}",
        "--maskvar",             f"{config['maskvar']}",
        "--preserve",            f"{config['preserve']}",
        "--bydist",              f"{config['bydist']}",
        "--bycontour",           f"{config['bycontour']}",
        "--nearbyblobs",         f"{config['nearbyblobs']}",
        "--timefilter",          f"{config['timefilter']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    if config['invert']:
        cmd.append("--invert")
    return cmd

def build_NodeFileCompose_command(config):
    """Build the NodeFileCompose command based on the provided configuration."""
    cmd = [
        "NodeFileCompose",
        "--in_nodefile",         f"{config['in_nodefile']}",
        "--in_nodefile_type",    f"{config['in_nodefile_type']}",
        "--in_fmt",              f"{config['in_fmt']}",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out_grid",            f"{config['out_grid']}",
        "--out_data",            f"{config['out_data']}",
        "--var",                 f"{config['var']}",
        "--varout",              f"{config['varout']}",
        "--op",                  f"{config['op']}",
        "--histogram",           f"{config['histogram']}",
        "--dx",                  f"{config['dx']}",
        "--resx",                f"{config['resx']}",
        "--resa",                f"{config['resa']}",
        "--fixlon",              f"{config['fixlon']}",
        "--fixlat",              f"{config['fixlat']}",
        "--max_time_delta",      f"{config['max_time_delta']}",
        "--lonname",             f"{config['lonname']}",
        "--latname",             f"{config['latname']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    if config['snapshots']:
        cmd.append("--snapshots")
    return cmd

def build_DetectBlobs_command(config):
    """Build the DetectBlobs command based on the provided configuration."""
    cmd = [
        "DetectBlobs",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out",                 f"{config['out']}",
        "--out_list",            f"{config['out_list']}",
        "--thresholdcmd",        f"{config['thresholdcmd']}",
        "--filtercmd",           f"{config['filtercmd']}",
        "--geofiltercmd",        f"{config['geofiltercmd']}",
        "--outputcmd",           f"{config['outputcmd']}",
        "--timefilter",          f"{config['timefilter']}",
        "--minlat",              f"{config['minlat']}",
        "--maxlat",              f"{config['maxlat']}",
        "--minabslat",           f"{config['minabslat']}",
        "--tagvar",              f"{config['tagvar']}",
        "--lonname",             f"{config['lonname']}",
        "--latname",             f"{config['latname']}",
        "--verbosity",           f"{config['verbosity']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    return cmd

def build_StitchBlobs_command(config):
    """Build the StitchBlobs command based on the provided configuration."""
    cmd = [
        "StitchBlobs",
        "--in",                  f"{config['in']}",
        "--in_list",             f"{config['in_list']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out",                 f"{config['out']}",
        "--out_list",            f"{config['out_list']}",
        "--var",                 f"{config['var']}",
        "--outvar",              f"{config['outvar']}",
        "--minsize",             f"{config['minsize']}",
        "--mintime",             f"{config['mintime']}",
        "--min_overlap_prev",    f"{config['min_overlap_prev']}",
        "--max_overlap_prev",    f"{config['max_overlap_prev']}",
        "--min_overlap_next",    f"{config['min_overlap_next']}",
        "--max_overlap_next",    f"{config['max_overlap_next']}",
        "--restrict_region",     f"{config['restrict_region']}",
        "--minlat",              f"{config['minlat']}",
        "--maxlat",              f"{config['maxlat']}",
        "--minlon",              f"{config['minlon']}",
        "--maxlon",              f"{config['maxlon']}",
        "--lonname",             f"{config['lonname']}",
        "--latname",             f"{config['latname']}",
        "--thresholdcmd",        f"{config['thresholdcmd']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    if config['flatten']:
        cmd.append("--flatten")
    return cmd

def build_BlobStats_command(config):
    """Build the BlobStats command based on the provided configuration."""
    cmd = [
        "BlobStats",
        "--in_file",             f"{config['in_file']}",
        "--in_list",             f"{config['in_list']}",
        "--findblobs",           f"{config['findblobs']}",
        "--in_connect",          f"{config['in_connect']}",
        "--out_file",            f"{config['out_file']}",
        "--var",                 f"{config['var']}",
        "--sumvar",              f"{config['sumvar']}",
        "--out",                 f"{config['out']}",
    ]
    if config['diag_connect']:
        cmd.append("--diag_connect")
    if config['regional']:
        cmd.append("--regional")
    if config['out_headers']:
        cmd.append("--out_headers")
    if config['out_fulltime']:
        cmd.append("--out_fulltime")
    return cmd

def build_Climatology_command(config):
    """Build the Climatology command based on the provided configuration."""
    cmd = [
        "Climatology",
        "--in_data",             f"{config['in_data']}",
        "--in_data_list",        f"{config['in_data_list']}",
        "--out_data",            f"{config['out_data']}",
        "--var",                 f"{config['var']}",
        "--memmax",              f"{config['memmax']}",
        "--period",              f"{config['period']}",
        "--type",                f"{config['type']}",
        "--temp_file_path",      f"{config['temp_file_path']}",
    ]
    if config['include_leap_days']:
        cmd.append("--include_leap_days")
    if config['missingdata']:
        cmd.append("--missingdata")
    if config['keep_temp_files']:
        cmd.append("--keep_temp_files")
    if config['verbose']:
        cmd.append("--verbose")
    return cmd

def build_FourierFilter_command(config):
    """Build the FourierFilter command based on the provided configuration."""
    cmd = [
        "FourierFilter",
        "--in_data",             f"{config['in_data']}",
        "--out_data",            f"{config['out_data']}",
        "--var",                 f"{config['var']}",
        "--preserve",            f"{config['preserve']}",
        "--dim",                 f"{config['dim']}",
        "--modes",               f"{config['modes']}",
    ]
    if config['include_leap_days']:
        cmd.append("--include_leap_days")
    if config['missingdata']:
        cmd.append("--missingdata")
    if config['keep_temp_files']:
        cmd.append("--keep_temp_files")
    if config['verbose']:
        cmd.append("--verbose")
    return cmd
