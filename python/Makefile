
mpispock:
	rm -f mpitest.out; srun -n 3 -o mpitest.out -A cli133 -t 00:10:00 -p ecp python3 miniweather_mpi.py 

serial:
	python3 miniweather_serial.py
	#python3 -m cProfile -s tottime miniweather_serial.py > prof_serial.txt

