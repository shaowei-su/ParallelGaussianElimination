/* this program is aimed at realizing parallel version of 
  gaussian elimination through Row Oriented Method. -- shaowei su
*/

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <pthread.h>

#include "hrtimer_x86.h"

#define RESET 1
#define NSIZE       128
#define VERIFY      1

#define SWAP(a,b)       {double tmp; tmp = a; a = b; b = tmp;}
#define SWAPINT(a,b)       {register int tmp; tmp = a; a = b; b = tmp;}
#define ABS(a)          (((a) > 0) ? (a) : -(a))

#define CHECK_CORRECTNESS 0

double **matrix,*B,*V,*C;
int *swap;
int task_num = 1;
int nsize = NSIZE;

pthread_mutex_t mut = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

/* code snippet from sor_pthread.c */

void barrier (int expect)
{
  static int arrived = 0;

  pthread_mutex_lock (&mut);    //lock

  arrived++;
  if (arrived < expect)
    pthread_cond_wait (&cond, &mut);
  else {
    arrived = 0;        // reset the barrier before broadcast is important
    pthread_cond_broadcast (&cond);
  }

  pthread_mutex_unlock (&mut);  //unlock
}


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

/*
    test case:
    | 2   1  -1 |  8 |
    | -3 -1  2  | -11|
    |-2   1  2  | -3 |

*/

void initCheck(int nsize)
{
    matrix[0][0] = 2;
    matrix[0][1] = 1;
    matrix[0][2] = -1;
    matrix[1][0] = -3;
    matrix[1][1] = -1;
    matrix[1][2] = 2;
    matrix[2][0] = -2;
    matrix[2][1] = 1;
    matrix[2][2] = 2;

    B[0] = 8;
    B[1] = -11;
    B[2] = -3;

    swap[0] = 0;
    swap[1] = 1;
    swap[2] = 2;
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
    In the first place, each thread will find its corresponding pivot row
    and then all threads are going to eliminate in parallel
*/

void *work_thread(void *id){
    int i,j,k;
    double pivotVal;
    double hrtime1, hrtime2;
    int task_id = *((int *) id);


    barrier(task_num); //wait for all threads to come and then start
    if(task_id == 0){
        hrtime1 = gethrtime_x86();
    }

    for(i=0; i<nsize; i++){
        if(task_id == i % task_num){
            getPivot(nsize,i); // select corresponding thread to find pivot in row
        }
        barrier(task_num); //wait for all threads finish 
        pivotVal = matrix[i][i];

        for (j = i + 1 ; j < nsize; j++){

            if(task_id == j % task_num){
                pivotVal = matrix[j][i];
                matrix[j][i] = 0.0;
                for (k = i + 1 ; k < nsize; k++){
                    matrix[j][k] -= pivotVal * matrix[i][k];
                }
                B[j] -= pivotVal * B[i];
            }
        }
        barrier(task_num);
    }
    hrtime2 = gethrtime_x86();

    if(task_id==0){
        printf("Hrtime = %f seconds\n", hrtime2 - hrtime1);
    }
    return NULL;
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
    pthread_t *tid;
    int *id;
    
    double mhz;
    hrtime_t hrcycle1, hrcycle2;

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
    
#ifdef CHECK_CORRECTNESS
    nsize = 3;
    allocate_memory(nsize);
    initCheck(nsize);
#else
    allocate_memory(nsize);
    initMatrix(nsize);
#endif
    
    hrcycle1 = gethrcycle_x86();
/* create thread*/
    id = (int *) malloc (sizeof (int) * task_num);
    tid = (pthread_t *) malloc (sizeof (pthread_t *) * task_num);
    if (!id || !tid)
        errexit ("out of shared memory");


    for(i=1; i<task_num; i++){
        id[i] = i;
        pthread_create(&tid[i], NULL, work_thread, &id[i]);
    }

    id[0] = 0;
    work_thread(&id[0]);

    for(i=1; i<task_num; i++){
        pthread_join(tid[i], NULL);
    }

#if VERIFY
    solveGauss(nsize);
#endif

    hrcycle2 = gethrcycle_x86();
    mhz = getMHZ_x86();

    printf("Here hrcycle = %lld, mhz = %f HZ\n", hrcycle2 - hrcycle1, mhz);


#if VERIFY
    for(i = 0; i < nsize; i++)
        printf("%6.5f %5.5f\n",B[i],C[i]);
#endif
    
    return 0;
}
