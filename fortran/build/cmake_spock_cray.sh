#!/bin/bash

#source ${MODULESHOME}/init/bash
module reset
module load cray-parallel-netcdf cmake

export TEST_MPI_COMMAND="srun -n 3 -A cli133 -t 00:10:00 -p ecp"

./cmake_clean.sh

cmake -DCMAKE_Fortran_COMPILER="ftn"    \
      -DPNETCDF_PATH="${PNETCDF_DIR}"   \
      -DOPENMP_FLAGS="-h omp,noacc"   \
      -DOPENMP45_FLAGS="-h omp,noacc" \
      -DFFLAGS="-O3"                  \
      -DLDFLAGS=""                    \
      -DNX=2000 \
      -DNZ=1000 \
      -DSIM_TIME=5 \
      ..
