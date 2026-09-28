#!/bin/bash

#SBATCH --job-name=track_ETCs_era5
#SBATCH --nodes=1
#SBATCH --output=track_ETCs_era5.o%j
#SBATCH --error=track_ETCs_era5.e%j
#SBATCH --time=0:30:00
#SBATCH --qos=debug
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

#source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/etc_nocoldcoreonly_kmscale_hackathon

dataidir=/global/cfs/cdirs/wcm_shr/hk25/era5/
dataodir=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/era5_tracking_etc_nocoldcoreonly
stitchdir=/global/cfs/cdirs/m1867/beharrop/kmscale_hackathon/stitch_nodes_data/
name=era5_ll025sc
etc_file=${stitchdir}/era5_tracking_full_etc_nocoldcoreonly/era5.etc_stitched_nodes.filtered_out_tcs.qs_filter_r30_d48.txt
tc_file=${stitchdir}/era5_tracking_full_etc_nocoldcoreonly/era5.tc_stitched_nodes.qs_filter_r15_d96.txt

sample_grid_file=/pscratch/sd/b/beharrop/kmscale_hackathon/hackathon_pre/era5_tracking_etc_nocoldcoreonly/TC_test_tracks_old_SN_era5_ll025sc.2020010100_2020010123.nc

#for year in {2019..2021}; do
#    for month in {01..12}; do
#        date_string="${year}${month}"
#        start_time="${year}-${month}-01T00"
#        last_day=$(date -d "${year}-${month}-01 +1 month -1 day" +%d)
#        end_time="${year}-${month}-${last_day}T18"
#
#	echo $start_time $end_time
#    done
#done

for year in {2019..2021}; do
    for month in {01..12}; do
	date_string="${year}${month}"
        start_time="${year}-${month}-01T00"
        last_day=$(date -d "${year}-${month}-01 +1 month -1 day" +%d)
        end_time="${year}-${month}-${last_day}T18"
	
	output_file=${dataodir}/TC_test_tracks_${name}.${date_string}.nc
	echo $output_file
	if [ ! -f "${output_file}" ]; then
	    python ETC_track_counter.py "$tc_file" --sample_grid_file "$sample_grid_file" \
		   --start_time "$start_time" --end_time "$end_time" --time_increment 6h \
		   --output_file $output_file --gcd_threshold 5.0 --stormtype TC --structured
	fi

	output_file=${dataodir}/ETC_test_tracks_${name}.${date_string}.nc
	echo $output_file
	if [ ! -f "${output_file}" ]; then
	    python ETC_track_counter.py "$etc_file" --sample_grid_file "$sample_grid_file" \
		   --start_time "$start_time" --end_time "$end_time" --time_increment 6h \
		   --output_file $output_file --gcd_threshold 10.0 --structured
	fi
    done
done


#for f in ${dataodir}/TC_test_tracks_old_SN_${name}.??????????_??????????.nc; do
#for f in ${dataidir}/TC_tracks_${name}.??????.nc; do
#    echo $f
#    date_string=${f:60:-3}
#    output_file=${dataodir}/TC_test_tracks_${name}.${date_string}.nc
#    echo $output_file
#    if [ ! -f "${output_file}" ]; then
#	python ETC_track_counter.py $tc_file $f $output_file --structured --gcd_threshold 5.0 --stormtype TC --binary_tag_name TC_count_index
#    fi
#done

#for f in ${dataidir}/ETC_test_tracks_${name}.??????.nc; do
#    echo $f
#    date_string=${f:66:-3}
#    output_file=${dataodir}/ETC_test_tracks_${name}.${date_string}.nc
#    echo $output_file
#    if [ ! -f "${output_file}" ]; then
#	python ETC_track_counter.py $etc_file $f $output_file --structured --gcd_threshold 10.0
#    fi
#done
    
echo All done
