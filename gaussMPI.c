/* this program is aimed at realizing MPI version of 
  gaussian elimination -- shaowei su
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include "mpi.h"

#define RESET 1
#define NSIZE       128
#define VERIFY      1

#define SWAP(a,b)       {double tmp; tmp = a; a = b; b = tmp;}
#define SWAPINT(a,b)       {register int tmp; tmp = a; a = b; b = tmp;}
#define ABS(a)          (((a) > 0) ? (a) : -(a))

MPI_Status status;

double **matrix,*B,*V,*C;
int *swap;
int nsize = NSIZE;
int task_num = 1;

//Allocate the needed arrays 

void allocate_memory(int size)
{
    double *tmp;
    int i;
    
    matrix = (double**)malloc(size*sizeof(double*));
    assert(matrix != NULL);
    tmp = (double*)malloc(size*size*sizeof(double));
    assert(tmp != NULL);
    
    for(i = 0; i < size; i++){
        matrix[i] = tmp;
        tmp = tmp + size;
    }
    
    B = (double*)malloc(size * sizeof(double));
    assert(B != NULL);
    V = (double*)malloc(size * sizeof(double));
    assert(V != NULL);
    C = (double*)malloc(size * sizeof(double));
    assert(C != NULL);
    swap = (int*)malloc(size * sizeof(int));
    assert(swap != NULL);
}


/* Initialize the matirx with some values that we know yield a
 * solution that is easy to verify. A correct solution should yield
 * -0.5, and 0.5 for the first and last C values consecutively, and 0
 * for the rest, though it should work regardless */
/* 4*4 matrix:
    2   2   2   2   0
    2   4   4   4   1
    2   4   6   6   2
    2   4   6   8   3
*/

void initMatrix(int nsize)
{
    int i,j;
    for(i = 0 ; i < nsize ; i++){
        for(j = 0; j < nsize ; j++) {
            matrix[i][j] = ((j < i )? 2*(j+1) : 2*(i+1));
        }
        B[i] = (double)i;
        swap[i] = i;
    }
}



/* Get the pivot row. If the value in the current pivot position is 0,
 * try to swap with a non-zero row. If that is not possible bail
 * out. Otherwise, make sure the pivot value is 1.0, and return. */

void getPivot(int nsize, int currow, int taskId, int numTasks)
{
    int i,irow;
    double big;
    double tmp;
    int rows_per_rank;

    rows_per_rank = nsize / numTasks;
    big = matrix[currow][currow];
    irow = currow;
    
    if (big == 0.0) {
        for(i = currow ; i < nsize; i++){
            tmp = matrix[i][currow];
            if (tmp != 0.0){
                big = tmp;
                irow = i;
                break;
            }
        }
    }
    
    if (big == 0.0){
        printf("The matrix is singular\n");
        exit(-1);
    }
    
    if (irow != currow){
        for(i = currow; i < nsize ; i++){
            SWAP(matrix[irow][i],matrix[currow][i]);
        }
        SWAP(B[irow],B[currow]);
        SWAPINT(swap[irow],swap[currow]);


    }
    
    
    {
        double pivotVal;
        pivotVal = matrix[currow][currow];
        
        if (pivotVal != 1.0){
            matrix[currow][currow] = 1.0;
            for(i = currow + 1; i < nsize; i++){
                matrix[currow][i] /= pivotVal;
            }
            B[currow] /= pivotVal;
        }
    }
}


/* Solve the equation. That is for a given A*B = C type of equation,
 * find the values corresponding to the B vector, when B, is all 1's */

void solveGauss(int nsize)
{
    int i,j;
    
    V[nsize-1] = B[nsize-1];
    for (i = nsize - 2; i >= 0; i --){
        V[i] = B[i];
        for (j = nsize - 1; j > i ; j--){
            V[i] -= matrix[i][j] * V[j];
        }
    }
    
    for(i = 0; i < nsize; i++){
      C[i] = V[i];//V[swap[i]];
    }
}

/*
    sendData() divide initialized matrix[][] and B[] into corresponding process
*/

void sendData (int taskId, int numTasks) {
    int src = 0;
    int dst;
    int rows_per_rank;

    rows_per_rank = nsize / numTasks;
    MPI_Status status;

    if (taskId == 0) {
        int i;
        for (i = 1; i < numTasks; i++) {
                MPI_Send(matrix[0], nsize*nsize, MPI_DOUBLE, i, i, MPI_COMM_WORLD); // send row
                MPI_Send(&B[0], nsize, MPI_DOUBLE, i, (i+nsize), MPI_COMM_WORLD); // send single B value
        }
    } else {
        int i;
        for (i = 1; i < numTasks; i++) {
            if (i == taskId) {
                MPI_Recv(matrix[0], nsize*nsize, MPI_DOUBLE, src, i, MPI_COMM_WORLD, &status);
                MPI_Recv(&B[0], nsize, MPI_DOUBLE, src, (i+nsize), MPI_COMM_WORLD, &status);
            }
        }
    }

}

void receiveData (int taskId, int numTasks) {
    int src = 0;
    int dst = 0;
    int rows_per_rank;

    rows_per_rank = nsize / numTasks;
    MPI_Status status;

    if (taskId == 0) {
        int i;
        for (i = 1; i < numTasks; i++) {
                MPI_Recv(matrix[i*rows_per_rank], rows_per_rank*nsize, MPI_DOUBLE, i, i, MPI_COMM_WORLD, &status); // send row
                MPI_Recv(&B[i*rows_per_rank], rows_per_rank, MPI_DOUBLE, i, (i+nsize), MPI_COMM_WORLD, &status); // send single B value
        }
    } else {
        int i;
        for (i = 1; i < numTasks; i++) {
            if (i == taskId) {
                MPI_Send(matrix[i*rows_per_rank], rows_per_rank*nsize, MPI_DOUBLE, dst, i, MPI_COMM_WORLD);
                MPI_Send(&B[i*rows_per_rank], rows_per_rank, MPI_DOUBLE, dst, (i+nsize), MPI_COMM_WORLD);
            }
        }
    }

}

/*
    After finding pivot value, every process will eliminate their share of rows
*/

void eliminate (int rowDeep, int taskId, int numTasks) {
    int row, col;
    double times;
    int rows_per_rank;

    rows_per_rank = nsize / numTasks;

    for (row = rowDeep + 1; row < nsize; row++) {
        if (row / rows_per_rank == taskId) {
            times = matrix[row][rowDeep] / matrix[rowDeep][rowDeep];
            for (col = rowDeep; col < nsize; col++) {
                matrix[row][col] -= matrix[rowDeep][col] * times;
            }
            B[row] -= B[rowDeep] * times;
        }
    }
}

extern char * optarg;
/* code snippet from sor_thread.c */
void
errexit (const char *err_str)
{
  fprintf (stderr, "%s", err_str);
  exit (1);
}


int main(int argc,char *argv[])
{
    int i;
    int rowDeep = 0;
    int MPI_task_id; // determine which processor it is
    int MPI_num_tasks; // determine how many processor there are
    int MPI_msg_type; // message type
    int rows_per_rank;


    MPI_Status status;

    double time_s, time_e;

    while((i = getopt(argc,argv,"s:p:")) != -1){
        switch(i){
            case 's':
                {
                    int s;
                    s = atoi(optarg);
                    if (s > 0){
                        nsize = s;
                    } else {
                        fprintf(stderr,"Entered size is negative, hence using the default (%d)\n",(int)NSIZE);
                    }
                }
                break;
            case 'p':
                {
                    int p;
                    p = atoi(optarg);
                    if (p > 0){
                        task_num = p;
                    } else {
                        fprintf(stderr,"Entered task number is negative, hence using the default value: 1\n");
                    }
                }
                break;
            default:
                assert(0);
                break;
        }
    } 

    allocate_memory(nsize);
    MPI_Init(&argc, &argv); // initialize the MPI function
    MPI_Comm_rank(MPI_COMM_WORLD, &MPI_task_id); // slave ID
    MPI_Comm_size(MPI_COMM_WORLD, &MPI_num_tasks);
    rows_per_rank = nsize / MPI_num_tasks;
    printf("I am %d, and now rows_per_rank = %d\n", MPI_task_id, rows_per_rank);

    if (MPI_task_id == 0) {

        printf("Now start...\n");
        initMatrix(nsize);

        time_s = MPI_Wtime(); // start count of time
    }
    sendData(MPI_task_id, MPI_num_tasks); // divide matrix to different ranks
    MPI_Barrier(MPI_COMM_WORLD);
    for (rowDeep = 0; rowDeep < nsize; rowDeep++) {

        int slv;
        slv = rowDeep / rows_per_rank;
        if (MPI_task_id == slv) {
            getPivot(nsize, rowDeep, slv, MPI_num_tasks); // each rank is responsible to find pivot value of certain rows
        }
        MPI_Bcast(matrix[rowDeep], nsize, MPI_DOUBLE, slv, MPI_COMM_WORLD);  
        MPI_Bcast(&B[rowDeep], 1, MPI_DOUBLE, slv, MPI_COMM_WORLD);
        MPI_Barrier(MPI_COMM_WORLD);

        eliminate(rowDeep, MPI_task_id, MPI_num_tasks); // eliminate shares
        MPI_Barrier(MPI_COMM_WORLD); // wait for all ranks to finish
    }

    receiveData(MPI_task_id, MPI_num_tasks);
    if (MPI_task_id == 0) {
        time_e = MPI_Wtime();
        printf("MPI gaussian elimination takes %f s\n", time_e - time_s);
#if VERIFY
    solveGauss(nsize);
#endif



#if VERIFY

    for(i = 0; i < nsize; i++) {
        printf("%6.5f %5.5f\n",B[i],C[i]);            
    }
#endif

    free(matrix);
    free(B);
    free(V);
    free(C);
    free(swap); 
    }

    MPI_Finalize();

    
    return 0;
}
