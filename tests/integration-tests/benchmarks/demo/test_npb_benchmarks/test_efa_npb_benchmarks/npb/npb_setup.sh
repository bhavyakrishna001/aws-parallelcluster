#!/bin/bash
set -e

wget https://www.nas.nasa.gov/assets/npb/NPB3.3.1.tar.gz
tar zxvf NPB3.3.1.tar.gz
cp make.def NPB3.3.1/NPB3.3-MPI/config/
cp suite.def NPB3.3.1/NPB3.3-MPI/config/
cd NPB3.3.1/NPB3.3-MPI && make suite
