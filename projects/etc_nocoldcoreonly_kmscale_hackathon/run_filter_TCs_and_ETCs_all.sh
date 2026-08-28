#!/bin/bash

#SBATCH --job-name=filter_TCs_ETCs_all
#SBATCH --nodes=1
#SBATCH --output=filter_TCs_ETCs_all.o%j
#SBATCH --error=filter_TCs_ETCs_all.e%j
#SBATCH --time=0:10:00
#SBATCH --qos=debug
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

module load conda
conda activate easy

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/etc_nocoldcoreonly_kmscale_hackathon/

datadir=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/

# CASESM2
tc_file=${datadir}/casesm2_10km_nocumulus_tracking_etc_nocoldcoreonly/casesm2_10km_nocumulus_hp8.tc_stitched_nodes.txt
etc_file=${datadir}/casesm2_10km_nocumulus_tracking_etc_nocoldcoreonly/casesm2_10km_nocumulus_hp8.etc_stitched_nodes.txt
filt_file=${datadir}/casesm2_10km_nocumulus_tracking_etc_nocoldcoreonly/casesm2_10km_nocumulus_hp8.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi

# ERA5
tc_file=${datadir}/era5_tracking_etc_nocoldcoreonly/era5.tc_stitched_nodes.txt
etc_file=${datadir}/era5_tracking_etc_nocoldcoreonly/era5.etc_stitched_nodes.txt
filt_file=${datadir}/era5_tracking_etc_nocoldcoreonly/era5.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi

# ERA5 full
tc_file=${datadir}/era5_tracking_full_etc_nocoldcoreonly/era5.tc_stitched_nodes.txt
etc_file=${datadir}/era5_tracking_full_etc_nocoldcoreonly/era5.etc_stitched_nodes.txt
filt_file=${datadir}/era5_tracking_full_etc_nocoldcoreonly/era5.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi

# ICON
tc_file=${datadir}/icon_d3hp003_1year_tracking_etc_nocoldcoreonly/icon_d3hp003_hp8.tc_stitched_nodes.txt
etc_file=${datadir}/icon_d3hp003_1year_tracking_etc_nocoldcoreonly/icon_d3hp003_hp8.etc_stitched_nodes.txt
filt_file=${datadir}/icon_d3hp003_1year_tracking_etc_nocoldcoreonly/icon_d3hp003_hp8.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi

# NICAM
tc_file=${datadir}/nicam_gl11_tracking_etc_nocoldcoreonly/nicam_gl11_hp8.tc_stitched_nodes.txt
etc_file=${datadir}/nicam_gl11_tracking_etc_nocoldcoreonly/nicam_gl11_hp8.etc_stitched_nodes.txt
filt_file=${datadir}/nicam_gl11_tracking_etc_nocoldcoreonly/nicam_gl11_hp8.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi

# SCREAM
tc_file=${datadir}/screamv2_ne120_tracking_etc_nocoldcoreonly/screamv2_ne120_hp8.tc_stitched_nodes.txt
etc_file=${datadir}/screamv2_ne120_tracking_etc_nocoldcoreonly/screamv2_ne120_hp8.etc_stitched_nodes.txt
filt_file=${datadir}/screamv2_ne120_tracking_etc_nocoldcoreonly/screamv2_ne120_hp8.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi

# UM
tc_file=${datadir}/um_glm_n2560_RAL3p3_tracking_etc_nocoldcoreonly/um_glm_n2560_RAL3p3_hp8.tc_stitched_nodes.txt
etc_file=${datadir}/um_glm_n2560_RAL3p3_tracking_etc_nocoldcoreonly/um_glm_n2560_RAL3p3_hp8.etc_stitched_nodes.txt
filt_file=${datadir}/um_glm_n2560_RAL3p3_tracking_etc_nocoldcoreonly/um_glm_n2560_RAL3p3_hp8.etc_stitched_nodes.filtered_out_tcs.txt
if [ ! -f "${filt_file}" ]; then
    python filter_TCs_and_ETCs.py ${tc_file} ${etc_file} ${filt_file}
fi
