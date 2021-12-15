
DATA := /gpfs/alpine/cli133/scratch/grnydawn/output

view:
	python3 pyslabs_viewer.py ${DATA}

mpispock:
	rm -f mpitest.out; rm -rf ${DATA}; srun -n 3 -o mpitest.out -A cli133 -t 00:10:00 -p ecp python3 miniweather_mpi.py 

serspock:
	rm -rf ${DATA}; python3 miniweather_serial.py
	#python3 -m cProfile -s tottime miniweather_serial.py > prof_serial.txt

