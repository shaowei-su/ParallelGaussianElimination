#include <stdio.h>
#include <sys/time.h>
#include <stdlib.h>
#include <assert.h>
#include <omp.h>

#include "hrtimer_x86.h"

#define NSIZE       128
#define VERIFY      1

#define SWAP(a,b)       {double tmp; tmp = a; a = b; b = tmp;}
#define SWAPINT(a,b)       {register int tmp; tmp = a; a = b; b = tmp;}
#define ABS(a)          (((a) > 0) ? (a) : -(a))

double **matrix,*B,*V,*C;
int *swap;
int nsize = NSIZE;
int task_num = 1;
/* Allocate the needed arrays */

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

void getPivot(int nsize, int currow)
{
    int i,irow;
    double big;
    double tmp;
    
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


/* For all the rows, get the pivot and eliminate all rows and columns
 * for that particular pivot row. */

void computeGauss(int nsize)
{
    int i,j,k;
    double pivotVal;
    for(i = 0; i < nsize; i++){
        getPivot(nsize,i);
        #pragma omp parallel for num_threads(task_num) private(j, k, pivotVal) firstprivate(i)
        for (j = i + 1 ; j < nsize; j++){

            pivotVal = matrix[j][i];
            matrix[j][i] = 0.0;
            for (k = i + 1 ; k < nsize; k++){
                matrix[j][k] -= pivotVal * matrix[i][k];
            }
            B[j] -= pivotVal * B[i];
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

extern char * optarg;
int main(int argc,char *argv[])
{
    int i;
    int nsize = NSIZE;
    double hrtime1, hrtime2;
    
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
    

    initMatrix(nsize);
    hrtime1 = gethrtime_x86();
    computeGauss(nsize);
    hrtime2 = gethrtime_x86();
    printf("Hrtime = %f seconds\n", hrtime2 - hrtime1);

#if VERIFY
    solveGauss(nsize);
#endif
    
    
#if VERIFY
    for(i = 0; i < nsize; i++)
        printf("%6.5f %5.5f\n",B[i],C[i]);
#endif
        free(matrix);
    free(B);
    free(V);
    free(C);
    free(swap); 
    
    return 0;
}
