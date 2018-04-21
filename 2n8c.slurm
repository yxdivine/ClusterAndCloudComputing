#!/bin/bash
#SBATCH --time=00:10:00
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --cpus-per-task=1
#SBATCH -p physical
#SBATCH -o output_2n8c.txt

module load Python/3.5.2-goolf-2015a


mpirun -np 8 ./streamrun.py