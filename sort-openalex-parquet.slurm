#!/bin/bash
#SBATCH --job-name=openalex_sort
#SBATCH --account=def-yourpi              # CHANGE ME: your Nibi allocation (def-/rrg-/rpp-)
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=320G                        # works_authorships needs the most RAM — see notes below
#SBATCH --time=08:00:00
#SBATCH --output=logs/openalex_sort_%j.out
#SBATCH --error=logs/openalex_sort_%j.err
#SBATCH --mail-type=FAIL,END
#SBATCH --mail-user=you@institution.ca    # CHANGE ME

# =============================================================================
# sort-openalex-parquet.slurm
#
# Runs sort-openalex-parquet.py on the Nibi cluster (Digital Research
# Alliance of Canada).
#
# TEST MODE vs PRODUCTION MODE
# -----------------------------
# By default this script sorts into a NEW output directory, leaving your
# original Parquet files untouched, so you can validate the result before
# overwriting anything. Once you're confident, flip SORT_IN_PLACE=true below
# (or just point OUTPUT_DIR at the same path as PARQUET_DIR) to sort in place.
#
# Usage
# -----
#   1. Edit the CHANGE ME items below (account, email, paths)
#   2. mkdir -p logs
#   3. sbatch sort-openalex-parquet.slurm
#
#   # Sort only one table (e.g. while testing):
#   sbatch --export=ONLY_TABLE=works_authorships sort-openalex-parquet.slurm
#
#   # Override directories without editing the file:
#   sbatch --export=PARQUET_DIR=/scratch/$USER/parquet-files,OUTPUT_DIR=/scratch/$USER/parquet-files-sorted-test \
#       sort-openalex-parquet.slurm
# =============================================================================

set -euo pipefail

echo "[$(date)] Job started on $(hostname)"
echo "  Job ID      : $SLURM_JOB_ID"
echo "  CPUs        : $SLURM_CPUS_PER_TASK"
echo "  Mem         : $SLURM_MEM_PER_NODE MB"

# -----------------------------------------------------------------------
# Environment setup — Nibi uses the StdEnv module system
# -----------------------------------------------------------------------
module purge
module load StdEnv/2023
module load python/3.11
module load gcc arrow


# Activate your virtualenv (create once beforehand with:
#   python -m venv ~/envs/openalex
#   source ~/envs/openalex/bin/activate
#   pip install pyarrow
VENV_DIR="${VENV_DIR:-$HOME/envs/openalex}"
source "data_env/bin/activate"

# -----------------------------------------------------------------------
# Paths — CHANGE ME as needed, or override via --export= at submit time
# -----------------------------------------------------------------------
SCRATCH="/scratch/$USER"

export PARQUET_DIR="${PARQUET_DIR:-$SCRATCH/openalex/parquet-files}"

# Default: write to a separate test directory, original files untouched.
# Set SORT_IN_PLACE=true to overwrite PARQUET_DIR directly instead.
SORT_IN_PLACE="${SORT_IN_PLACE:-false}"
if [ "$SORT_IN_PLACE" = "true" ]; then
    export OUTPUT_DIR="$PARQUET_DIR"
else
    export OUTPUT_DIR="${OUTPUT_DIR:-$SCRATCH/openalex/parquet-files-sorted-test}"
fi

# Optional: restrict to a single table (e.g. for a quick test run)
export OPENALEX_ONLY="${ONLY_TABLE:-}"

# Optional: tune row-group size
export OPENALEX_ROW_GROUP_SIZE="${OPENALEX_ROW_GROUP_SIZE:-100000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "  PARQUET_DIR : $PARQUET_DIR"
echo "  OUTPUT_DIR  : $OUTPUT_DIR"
echo "  In place    : $SORT_IN_PLACE"
echo "  Only table  : ${OPENALEX_ONLY:-(all)}"

mkdir -p "$(dirname "$0")/logs" 2>/dev/null || true
mkdir -p "$OUTPUT_DIR"

# -----------------------------------------------------------------------
# Run
# -----------------------------------------------------------------------
python3 "$SCRIPT_DIR/sort-openalex-parquet.py"

echo "[$(date)] Job finished"

# -----------------------------------------------------------------------
# After a successful test run, promote the sorted output to production with:
#   rsync -av --remove-source-files "$OUTPUT_DIR/" "$PARQUET_DIR/"
# or simply re-run this script with SORT_IN_PLACE=true once you're satisfied.
# -----------------------------------------------------------------------
