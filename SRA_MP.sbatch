#!/bin/bash
#SBATCH -J SRA7
#SBATCH --partition comp72
#SBATCH -o SRA7_%j.out
#SBATCH --time 72:00:00 
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=32
module load python/3.12-anaconda
conda activate busco-3.12
python downloadMP.py
conda deactivate