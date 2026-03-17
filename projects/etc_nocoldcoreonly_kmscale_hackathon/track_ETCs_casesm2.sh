#!/bin/bash

#SBATCH --job-name=track_ETCs_casesm2
#SBATCH --nodes=1
#SBATCH --output=track_ETCs_casesm2.o%j
#SBATCH --error=track_ETCs_casesm2.e%j
#SBATCH --time=1:15:00
#SBATCH --qos=regular
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/etc_nocoldcoreonly_kmscale_hackathon

datadir=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/casesm2_10km_nocumulus_tracking_etc_nocoldcoreonly
name=casesm2_10km_nocumulus_hp8_H
etc_file=${datadir}/casesm2_10km_nocumulus_hp8.etc_stitched_nodes.txt
tc_file=${datadir}/casesm2_10km_nocumulus_hp8.tc_stitched_nodes.txt

year=2020
for month in {03..12}; do
    yyyymm=$year$month
    echo $yyyymm
    bin_masks_file=${datadir}/ETC_filt_nodes_${name}.${yyyymm}.nc
    output_file=${datadir}/ETC_test_tracks_${name}.${yyyymm}.nc
    python ETC_track_counter.py $etc_file $bin_masks_file $output_file --gcd_threshold 10.0
    bin_masks_file=${datadir}/TC_filt_nodes_${name}.${yyyymm}.nc
    output_file=${datadir}/TC_test_tracks_${name}.${yyyymm}.nc
    python ETC_track_counter.py $tc_file $bin_masks_file $output_file --gcd_threshold 5.0 --stormtype TC
done

year=2021
for month in {01..03}; do
    yyyymm=$year$month
    echo $yyyymm
    bin_masks_file=${datadir}/ETC_filt_nodes_${name}.${yyyymm}.nc
    output_file=${datadir}/ETC_test_tracks_${name}.${yyyymm}.nc
    python ETC_track_counter.py $etc_file $bin_masks_file $output_file --gcd_threshold 10.0
    bin_masks_file=${datadir}/TC_filt_nodes_${name}.${yyyymm}.nc
    output_file=${datadir}/TC_test_tracks_${name}.${yyyymm}.nc
    python ETC_track_counter.py $tc_file $bin_masks_file $output_file --gcd_threshold 5.0 --stormtype TC
done
    
echo All done
