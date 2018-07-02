#!/bin/bash
#SBATCH --partition=physical
#SBATCH --time=0-0:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1

# Load required modules
module load Python/3.5.2-goolf-2015a

# Execute Python Job on 1 node and 1 core
echo "Cluster and Cloud Computing Assignment1 using 1 node and 1 core"
time mpiexec python3 HPCInstagramGeoProcessingUsingMPI.py melbGrid.json bigInstagram.json

