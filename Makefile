
all: mpi omp

mpi: gaussMPI.c
		mpicc gaussMPI.c -o mpi

omp: gaussOPENMP.c hrtimer_x86.c hrtimer_x86.h
		gcc gaussOPENMP.c hrtimer_x86.c -o omp

clean:
	-rm mpi 
	-rm omp