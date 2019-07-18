#!/bin/bash
set -e

MPI_PROGRAM=$1
CORES=$2

module load openmpi
mkdir -p /shared/npb_results
OUT_FILE=/shared/npb_results/$(basename ${MPI_PROGRAM}).out
echo "" > ${OUT_FILE}
mpirun -np ${CORES} ${MPI_PROGRAM} >& ${OUT_FILE}
