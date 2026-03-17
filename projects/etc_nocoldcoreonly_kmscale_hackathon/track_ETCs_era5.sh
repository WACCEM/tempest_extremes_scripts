#!/bin/bash

#SBATCH --job-name=track_ETCs_era5
#SBATCH --nodes=1
#SBATCH --output=track_ETCs_era5.o%j
#SBATCH --error=track_ETCs_era5.e%j
#SBATCH --time=10:00:00
#SBATCH --qos=regular
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/etc_nocoldcoreonly_kmscale_hackathon

datadir=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/era5_tracking_etc_nocoldcoreonly
name=era5_ll025sc
etc_file=${datadir}/era5.etc_stitched_nodes.txt
tc_file=${datadir}/era5.tc_stitched_nodes.txt

for f in ${datadir}/TC_filt_nodes_${name}.??????????_??????????.nc; do
    echo $f
    date_string=${f:98:-3}
    output_file=${datadir}/TC_test_tracks_${name}.${date_string}.nc
    echo $output_file
    if [ ! -f "${output_file}" ]; then
	python ETC_track_counter.py $tc_file $f $output_file --structured --gcd_threshold 5.0 --stormtype TC
    fi
done

for f in ${datadir}/ETC_filt_nodes_${name}.??????????_??????????.nc; do
    echo $f
    date_string=${f:98:-3}
    output_file=${datadir}/ETC_test_tracks_${name}.${date_string}.nc
    echo $output_file
    if [ ! -f "${output_file}" ]; then
	python ETC_track_counter.py $etc_file $f $output_file --structured --gcd_threshold 10.0
    fi
done
    
echo All done
