#!/bin/bash

#SBATCH --job-name=filter_TCs_ETCs_scream
#SBATCH --nodes=1
#SBATCH --output=filter_TCs_ETCs_scream.o%j
#SBATCH --error=filter_TCs_ETCs_scream.e%j
#SBATCH --time=0:10:00
#SBATCH --qos=debug
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

module load conda
conda activate easy

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/etc_nocoldcoreonly_kmscale_hackathon/

datadir=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/screamv2_ne120_tracking_etc_nocoldcoreonly/
tc_file=${datadir}/screamv2_ne120_hp8.tc_stitched_nodes.txt
etc_file=${datadir}/screamv2_ne120_hp8.etc_stitched_nodes.txt
filt_file=${datadir}/screamv2_ne120_hp8.etc_stitched_nodes.filtered_out_tcs.txt

python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
