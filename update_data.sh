#!/bin/bash
# SBATCH --job-name=openalex_update
# SBATCH --core-per-task=4
# SBATCH --mem=32G
# SBATCH --time=24:00:00
# SBATCH --output=update_data_%j.out

module load gcc arrow

# Download du dernier snapshot OpenAlex 
aws s3 sync "s3://openalex/data/" "/project/def-yacineb/openalex_snapshot/data/" --delete

# Extraction Parquet de seulement les fichiers récents
python3 flatten-openalex-parquet.py --input_dir /project/def-yacineb/openalex_snapshot/data/ --output_dir /project/def-yacineb/openalex_snapshot/parquet/ --recent_only