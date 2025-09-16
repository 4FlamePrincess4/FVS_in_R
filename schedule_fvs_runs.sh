#!/bin/bash
#SBATCH --job-name=<%= job.name %>
#SBATCH --account=wildland_fire_smoke_tradeoff
#SBATCH --output=<%= log.file %>
#SBATCH --error=<%= log.file %>
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=4G
#SBATCH --partition=ceres
#SBATCH --ntasks=1
#SBATCH --mail-user=laurel.sindewald@usda.gov
#SBATCH --mail-type=BEGIN
#SBATCH --mail-type=END
#SBATCH --mail-type=FAIL

# Activate conda environment
module load miniconda
source activate r_env2

/home/laurel.sindewald/.conda/envs/r_env2/bin/Rscript run_fvs_slurm.R -e 'batchtools::doJobCollection("<%= uri %>")'
