#!/bin/bash
#SBATCH --job-name=gtInfo
#SBATCH --account=phoenix-testusers
#SBATCH -N1 --ntasks-per-node=4
#SBATCH --mem-per-cpu=7G
#SBATCH -t02:00:00
#SBATCH -qinferno
#SBATCH -o ./Report/output.%a.out
cd $SLURM_SUBMIT_DIR
source /storage/coda1/p-pace-user/0/ksmith609/test_installs/neo4j_venv/bin/activate
python author_doi.py