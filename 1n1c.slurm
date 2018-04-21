#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH -p physical
#SBATCH -o output_1n1c.txt

module load Python/3.5.2-goolf-2015a


mpirun -np 1 ./streamrun.py