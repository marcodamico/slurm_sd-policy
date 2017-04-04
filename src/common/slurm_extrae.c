#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>

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
//char *trace_pcf = "slurm_workload_trace.pcf";
//char *trace_row = "slurm_workload_trace.row";
long int trace_prv_header_offset = 0;
int n_cpus = 0;
int first_job = 0;
extrae_thread_t *extrae_threads;
int node_number = 0;

/* Slurmctld init the first line of paraver prv file  */
int slurmctld_extrae_trace_init()
{
	time_t now;
	struct tm *tm_now;
	FILE *trace_fp;

	debug("In slurmctld_extrae_trace_init");

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

	fprintf(trace_fp, "#PARAVER (%d/%d/%d at %d:%d):", tm_now->tm_mday, tm_now->tm_mon, tm_now->tm_year + 1900, tm_now->tm_hour, tm_now->tm_min);
	gettimeofday(&init_time, NULL);
	trace_prv_header_offset = ftell(trace_fp);
	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
	//use second line to store init_time, as start time in header is limited to hh:mm
	fprintf(trace_fp, "\n");
	fflush(trace_fp);
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
	int i, j, first;
	ListIterator itr = NULL;
	struct job_record *job_ptr = NULL;

	debug("In slurmctld_extrae_trace_fini");
	
	gettimeofday(&fini_time, NULL);

	slurm_mutex_lock(&extrae_lock);

	if (!trace_initialized) {
		slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
	}
	trace_fp = fopen(trace_prv, "r+");
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
		first = 0;
		fprintf(trace_fp, ":%d(", job_ptr->details->num_tasks);
		for(i = 0; i < node_record_count; i++) {
			if (!bit_test(job_ptr->node_bitmap, i))
                                        continue;
			for(j = 0; j < job_ptr->details->ntasks_per_node; j++)
				//threads for each task
				if(!first) {
					fprintf(trace_fp, "%d:%d", job_ptr->details->cpus_per_task, i + 1);
					first = 1;
				}
				else
					fprintf(trace_fp, ",%d:%d", job_ptr->details->cpus_per_task, i + 1);
			}
		fprintf(trace_fp, ")");
	}
	fprintf(trace_fp, "\n");
	fflush(trace_fp);
	fclose(trace_fp);
        slurm_mutex_unlock(&extrae_lock);
	return SLURM_SUCCESS;
}

int slurmd_extrae_trace_init(int ncpus)
{
        int i;

	debug("In slurmd_extrae_trace_init");

	if(trace_initialized)
		return SLURM_SUCCESS;

        gettimeofday(&init_time, NULL);
        n_cpus = ncpus;
	extrae_threads = xmalloc(sizeof(extrae_thread_t) * n_cpus);
        for(i = 0; i < n_cpus; i++) {
                extrae_threads[i].job_id = -1;
	}
	trace_initialized = 1;
        return SLURM_SUCCESS;
}

int slurmd_extrae_trace_fini()
{
	printf("In slurmd_extrae_trace_fini");
	xfree(extrae_threads);
        return SLURM_SUCCESS;
}

int slurmd_get_next_extrae_thread(int job_id, int task_id)
{
	int i,j;
	//one extrae_thread per cpu max
	for(i = 0; i < n_cpus; i++) {
		for(j = 0; j < n_cpus; j++) {
			if(extrae_threads[j].job_id != job_id ||
			   extrae_threads[j].task_id != task_id)
				continue;
			if(extrae_threads[j].thread_id == (i + 1))
				break;
		}
		if(j == n_cpus || extrae_threads[j].thread_id != (i + 1) ||
		   extrae_threads[j].job_id != job_id 	  ||
		   extrae_threads[j].task_id != task_id)
			return i + 1;
	}
	
	return -1;
}

static void _start_thread(int cpu_id, int app_id, int task_id, int th_id)
{
	debug("_start_thread\n");

        struct timeval fini_time;
        gettimeofday(&fini_time, NULL);
        long elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;
        sprintf(extrae_threads[cpu_id].entry, "1:%d:%d:%d:%d:%ld", cpu_id + 1, app_id, task_id, th_id, elapsed);
}

static int _stop_thread(int cpu_id)
{
	FILE *trace_fp;
        struct timeval fini_time;
	int len;
	long elapsed;

	debug("In _stop_thread\n");

        gettimeofday(&fini_time, NULL);
        elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;
	len = strlen(extrae_threads[cpu_id].entry);
	sprintf(extrae_threads[cpu_id].entry + len, ":%ld:%d", elapsed, RUNNING);

        slurm_mutex_lock(&extrae_lock);

        trace_fp = fopen(trace_prv,"a");
        if (trace_fp == NULL) {
                slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
        }
        fprintf(trace_fp, "%s\n", extrae_threads[cpu_id].entry);
	extrae_threads[cpu_id].job_id = -1;
        fflush(trace_fp);
	fclose(trace_fp);

        slurm_mutex_unlock(&extrae_lock);
        return SLURM_SUCCESS;
}

static int _print_extrae_threads()
{
	int i;

	for(i = 0; i < n_cpus; i++)
		if(extrae_threads[i].job_id != -1)
			debug(" %d %d %s", i, extrae_threads[i].job_id, extrae_threads[i].entry);
	return 0;
}

int slurmd_extrae_start_thread(int job_id, int cpu_id, int task_id, int th_id)
{
	debug("In slurmd_extrae_start_thread\n");

	if(slurmd_extrae_stop_thread(cpu_id) != SLURM_SUCCESS) {
		debug("Error in slurmd_extrae_stop_thread");
		return SLURM_ERROR;
	}
	
	if(!first_job)
		first_job = job_id - 1;
	int app_id = job_id - first_job;
	_start_thread(cpu_id, app_id, task_id, th_id);
	extrae_threads[cpu_id].job_id = job_id;
	extrae_threads[cpu_id].task_id = task_id;
	extrae_threads[cpu_id].thread_id = th_id;
	_print_extrae_threads();
	return SLURM_SUCCESS;
}

int slurmd_extrae_stop_thread(int cpu_id)
{
	debug("In slurmd_extrae_stop_thread\n");

	if(extrae_threads[cpu_id].job_id == -1)
		return SLURM_SUCCESS;
	if(_stop_thread(cpu_id) != SLURM_SUCCESS)
		return SLURM_ERROR;
	extrae_threads[cpu_id].job_id = -1;
	_print_extrae_threads();
	return SLURM_SUCCESS;
}
