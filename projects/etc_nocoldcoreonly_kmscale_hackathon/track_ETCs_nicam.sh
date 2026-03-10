#!/bin/bash

#SBATCH --job-name=track_ETCs_nicam
#SBATCH --nodes=1
#SBATCH --output=track_ETCs_nicam.o%j
#SBATCH --error=track_ETCs_nicam.e%j
#SBATCH --time=0:45:00
#SBATCH --qos=regular
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/kmscale_hackathon

datadir=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/nicam_gl11_tracking
name=nicam_gl11_hp8_H
storm_file=${datadir}/nicam_gl11_hp8.etc_stitched_nodes.txt

year=2020
for month in {03..12}; do
    yyyymm=$year$month
    echo $yyyymm
    bin_masks_file=${datadir}/ETC_filt_nodes_${name}.${yyyymm}.nc
    output_file=${datadir}/ETC_test_tracks_${name}.${yyyymm}.nc
    python ETC_track_counter.py $storm_file $bin_masks_file $output_file
done

year=2021
for month in {01..02}; do
    yyyymm=$year$month
    echo $yyyymm
    bin_masks_file=${datadir}/ETC_filt_nodes_${name}.${yyyymm}.nc
    output_file=${datadir}/ETC_test_tracks_${name}.${yyyymm}.nc
    python ETC_track_counter.py $storm_file $bin_masks_file $output_file
done
    
echo All done
