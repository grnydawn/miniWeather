
#DATA := /gpfs/alpine/cli133/scratch/grnydawn/output
DATA := ./output

view:
	python3 simpdf_viewer.py ${DATA}

mpispock:

	rm -f mpitest.out; srun -n 3 -o mpitest.out -A cli133 -t 00:10:00 -p ecp python3 miniweather_mpi.py 
mpimac:
	mpirun -n 3 -- python3 miniweather_mpi.py 

serial:
	python3 miniweather_serial.py
	#python3 -m cProfile -s tottime miniweather_serial.py > prof_serial.txt

