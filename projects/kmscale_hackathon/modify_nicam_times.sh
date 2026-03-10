#!/bin/bash

source /global/common/software/e3sm/anaconda_envs/load_latest_e3sm_unified_pm-cpu.sh

# List of files
files=(
  "nicam_gl11_ivt_hp8_PT3H.202003.nc"
  "nicam_gl11_ivt_hp8_PT3H.202004.nc"
  "nicam_gl11_ivt_hp8_PT3H.202005.nc"
  "nicam_gl11_ivt_hp8_PT3H.202006.nc"
  "nicam_gl11_ivt_hp8_PT3H.202007.nc"
  "nicam_gl11_ivt_hp8_PT3H.202008.nc"
  "nicam_gl11_ivt_hp8_PT3H.202009.nc"
  "nicam_gl11_ivt_hp8_PT3H.202010.nc"
  "nicam_gl11_ivt_hp8_PT3H.202011.nc"
  "nicam_gl11_ivt_hp8_PT3H.202012.nc"
  "nicam_gl11_ivt_hp8_PT3H.202101.nc"
  "nicam_gl11_ivt_hp8_PT3H.202102.nc"
)

# Loop through files and echo each one
for file in "${files[@]}"; do
    echo "$file"
    # Use `ncdump` to extract the attribute
    units=$(ncdump -h "$file" | grep 'time:units' | awk -F'"' '{print $2}')
    new_time_attr="${units:0:22} 00:00:00"
  
    # Use the extracted units to update the time
    echo "ncatted -a units,time,o,c,"${new_time_attr}" $file"
    ncatted -a units,time,o,c,"${new_time_attr}" $file
done

    
