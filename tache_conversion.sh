#!/bin/bash
#SBATCH --account=def-yacineb
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --output=conversion_%j.out       

module load gcc arrow

# Activer l'environnement virtuel
source data_env/bin/activate

# Faire rouler le script de conversion
python flatten-openalex-parquet.py
