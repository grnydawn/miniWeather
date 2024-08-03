#!/bin/bash

export TEST_MPI_COMMAND="mpirun -n 1 "

./cmake_clean.sh

PNETCDF_ROOT=/Users/8yk/opt/pnetcdf/1.13.0

cmake -DCMAKE_Fortran_COMPILER=mpif90 \
      -DFFLAGS="-O3 -ffree-line-length-none -I${PNETCDF_ROOT}/include"   \
      -DLDFLAGS="-L${PNETCDF_ROOT}/lib -lpnetcdf"                        \
      -DNX=1000 \
      -DNZ=500  \
      -DSIM_TIME=100 \
      -DOUT_FREQ=200 \
      ..

