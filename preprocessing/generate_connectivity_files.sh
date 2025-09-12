#!/bin/bash

# This is best run on an interactive node on Perlmutter.

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

for zoom in {0..10}; do
    
    in_grid=healpix_grid_zoom_${zoom}_format_exodus.nc
    out_connect=connect_${in_grid}.txt

    if [ ! -f "${out_connect}" ]; then
	echo Generating connectivity file for zoom level $zoom
	GenerateConnectivityFile \
	    --in_mesh ${in_grid} \
	    --out_connect ${out_connect} \
	    > connectivity_file_generation_output_using_${in_grid}.log
    fi
done




