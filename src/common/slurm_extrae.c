#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "src/common/slurm_extrae.h"
#include "src/common/list.h"
#include "src/common/xmalloc.h"
#include "src/common/node_conf.h"
#include "src/slurmctld/slurmctld.h"

//this lock is between threads, I should implement a inter-process lock
static pthread_mutex_t extrae_lock = PTHREAD_MUTEX_INITIALIZER;

struct timeval init_time;
int trace_initialized = 0;
char *trace_prv = "slurm_workload_trace.prv";
char *trace_pcf = "slurm_workload_trace.pcf";
char *trace_row = "slurm_workload_trace.row";
off_t trace_prv_header_offset = 0;
int n_cpus = 0;
int first_job = 0;
extrae_thread_t *extrae_threads;

/* Slurmctld init the first line of paraver prv file  */
int slurmctld_extrae_trace_init()
{
	time_t now;
	struct tm *tm_now;
	FILE *trace_fp;
	time(&now);
	tm_now = localtime(&now);
	
	slurm_mutex_lock(&extrae_lock);
	if (trace_initialized) {
		slurm_mutex_unlock(&extrae_lock);
		return SLURM_SUCCESS;
	}
	
	trace_fp = fopen(trace_prv,"w");
	if(!trace_fp) {
		slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
        }

	fprintf(trace_fp, "#PARAVER (%d/%d/%d at %d:%d):", tm_now->tm_mday, tm_now->tm_mon, tm_now->tm_year, tm_now->tm_hour, tm_now->tm_min);
	gettimeofday(&init_time, NULL);
	trace_prv_header_offset = ftell(trace_fp);
	//use second line to store init_time, as start time in header is limited to hh:mm
	fprintf(trace_fp, "\n");
	fclose(trace_fp);
	trace_initialized = 1;
	slurm_mutex_unlock(&extrae_lock);
	return SLURM_SUCCESS;
}

/* Slurmctld complete at the end of the execution the first line of 
 * paraver prv file
 */
int slurmctld_extrae_trace_fini(List job_list, struct node_record *node_table, int node_record_count)
{
	struct timeval fini_time;
	FILE *trace_fp;
	long elapsed;
	int i, threads;
	ListIterator itr = NULL;
	struct job_record *job_ptr = NULL;

	gettimeofday(&fini_time, NULL);

	slurm_mutex_lock(&extrae_lock);

	if (!trace_initialized) {
		slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
	}
	trace_fp = fopen(trace_prv,"w+");
	if (trace_fp == NULL) {
		slurm_mutex_unlock(&extrae_lock);
		return SLURM_ERROR;
	}
	fseek(trace_fp, trace_prv_header_offset, SEEK_SET);

	elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;	

	fprintf(trace_fp, "%ld:", elapsed);
	fprintf(trace_fp, "%d(", node_record_count);
	/* print cpus per each node */
	for(i = 0; i < node_record_count - 1; i++) {
		fprintf(trace_fp, "%d,", node_table[i].cpus);
	}
	fprintf(trace_fp, "%d):%d", node_table[i].cpus, list_count(job_list));
	/* print app list */
	itr = list_iterator_create(job_list);	
	while((job_ptr = list_next(itr))) {
		//ntasks
		fprintf(trace_fp, ":%d(", job_ptr->details->num_tasks);
		for(i = 0; i < node_record_count; i++)
			if (!bit_test(job_ptr->node_bitmap, i))
                        	continue;
			//threads for each node
			threads = job_ptr->details->ntasks_per_node * job_ptr->details->cpus_per_task;
			fprintf(trace_fp, "%d:%d,", threads, i);
		fprintf(trace_fp, ")");
	}
	fclose(trace_fp);
        slurm_mutex_unlock(&extrae_lock);
	return SLURM_SUCCESS;
}

int slurmd_extrae_trace_init(int ncpus)
{
        int i;
	if(trace_initialized)
		return SLURM_SUCCESS;

        gettimeofday(&init_time, NULL);
        n_cpus = ncpus;
        extrae_threads = xmalloc(sizeof(extrae_thread_t) * n_cpus);
        for(i = 0; i < n_cpus; i++)
                extrae_threads[i].job_id = -1;
	trace_initialized = 1;
        return SLURM_SUCCESS;
}

int slurmd_extrae_trace_fini()
{
	xfree(extrae_threads);
        return SLURM_SUCCESS;
}

static void _start_thread(int cpu_id, int app_id, int task_id, int th_id)
{
        struct timeval fini_time;
        gettimeofday(&fini_time, NULL);
        long elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;
        sprintf(extrae_threads[cpu_id].entry, "1:%d:%d:%d:%d:%ld", cpu_id, app_id, task_id, th_id, elapsed);
}

static int _stop_thread(int cpu_id)
{
	FILE *trace_fp;
        struct timeval fini_time;
        gettimeofday(&fini_time, NULL);
        long elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;

        sprintf(extrae_threads[cpu_id].entry, "%s:%ld:%d", extrae_threads[cpu_id].entry, elapsed, RUNNING);

        slurm_mutex_lock(&extrae_lock);

        trace_fp = fopen(trace_prv,"a");
        if (trace_fp == NULL) {
                slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
        }
        fprintf(trace_fp, "%s\n", elapsed);
        fclose(trace_fp);

        slurm_mutex_unlock(&extrae_lock);
        return SLURM_SUCCESS;
}

int slurmd_extrae_start_thread(int job_id, int cpu_id, int task_id, int th_id)
{
	if(slurmd_extrae_stop_thread(cpu_id))
		return SLURM_ERROR;
	
	if(!first_job)
		first_job = job_id;
	int app_id = job_id - first_job;
	_start_thread(cpu_id, app_id, task_id, th_id);
	return SLURM_SUCCESS;
}

int slurmd_extrae_stop_thread(int cpu_id)
{
	if(extrae_threads[cpu_id].job_id == -1)
		return SLURM_SUCCESS;
	if(_stop_thread(cpu_id))
		return SLURM_ERROR;
	extrae_threads[cpu_id].job_id = -1;
	return SLURM_SUCCESS;
}
