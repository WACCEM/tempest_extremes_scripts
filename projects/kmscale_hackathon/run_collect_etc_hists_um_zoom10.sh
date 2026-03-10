#!/bin/bash

#SBATCH --job-name=collect_etc_hists_um_zoom10
#SBATCH --nodes=1
#SBATCH --output=collect_etc_hists_um_zoom10.o%j
#SBATCH --error=collect_etc_hists_um_zoom10.e%j
#SBATCH --time=6:30:00
#SBATCH --qos=regular
#SBATCH --account=m1867
#SBATCH --constraint=cpu
#SBATCH --mail-type=end,fail
#SBATCH --mail-user=bryce.harrop@pnnl.gov

module load conda
conda activate easy

cd /pscratch/sd/b/beharrop/kmscale_hackathon/tempest_extremes_scripts/projects/kmscale_hackathon/

python collect_ETC_histograms.py --case um --zoom_level 10
