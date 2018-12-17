#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>
#include <string.h>

#include "slurm/slurm.h"

#include "src/common/slurm_extrae.h"
#include "src/common/list.h"
#include "src/common/xmalloc.h"
#include "src/common/node_conf.h"
#include "src/common/read_config.h"
#include "src/slurmctld/slurmctld.h"

#define MAX_STR_LEN 80

/* Common vars */

//this lock is between threads, I should implement a inter-process lock
static pthread_mutex_t extrae_lock = PTHREAD_MUTEX_INITIALIZER;
struct timeval init_time;
int trace_initialized = 0;
char *trace_body = "slurm_workload_body";
char *trace_body_slurmctld = "slurm_workload_body_slurmctld";
char trace_body_with_id[MAX_STR_LEN+1];
char *time_file = "slurm_extrae_init_time";
int first_job = -1;
FILE *body_fp = NULL;
/* SLURMCTLD variables */
char *trace_prv = "slurm_workload_trace.prv";
List extrae_job_list = NULL;
FILE *trace_fp = NULL;

/* SLURMD variables */
int n_cpus = 0;
extrae_thread_t *extrae_threads;
int base_cpu_id = -1;
int node_id = -1;

void _destroy_extrae_job_t(void *job)
{
	extrae_job_t *_job = (extrae_job_t *) job;
	FREE_NULL_BITMAP(_job->node_bitmap);
}

/* Slurmctld init the first line of paraver prv file  */
int slurmctld_extrae_trace_init()
{
	time_t now;
	struct tm *tm_now;
	FILE *time_fp;

	debug("In slurmctld_extrae_trace_init");
	
	time(&now);
	tm_now = localtime(&now);
	
	slurm_mutex_lock(&extrae_lock);
	if (trace_initialized) {
		slurm_mutex_unlock(&extrae_lock);
		return SLURM_SUCCESS;
	}
	
	slurm_ctl_conf_t *conf = slurm_conf_lock();
	first_job = conf->first_job_id;
	slurm_conf_unlock(); 
	
	extrae_job_list = list_create(_destroy_extrae_job_t);
	
	trace_fp = fopen(trace_prv,"w");
	if(!trace_fp) {
		slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
        }

	fprintf(trace_fp, "#PARAVER (%d/%d/%d at %d:%d):", tm_now->tm_mday, tm_now->tm_mon, tm_now->tm_year + 1900, tm_now->tm_hour, tm_now->tm_min);
	
	gettimeofday(&init_time, NULL);
	time_fp = fopen(time_file, "w");
	if(time_fp == NULL)
		return SLURM_ERROR;
	fwrite(&init_time, sizeof(init_time), 1, time_fp);
	fclose(time_fp);
//	trace_prv_header_offset = ftell(trace_fp);
//	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
//	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
//	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
//	fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");fprintf(trace_fp,"\n\n\n\n\n\n\n\n\n\n");
	//use second line to store init_time, as start time in header is limited to hh:mm
//	fprintf(trace_fp, "\n");

	body_fp = fopen(trace_body_slurmctld, "w");

	trace_initialized = 1;
	slurm_mutex_unlock(&extrae_lock);
	return SLURM_SUCCESS;
}

int _merge_in_file(FILE *fp1, FILE *fp2)
{
	char str[MAX_STR_LEN + 1];
	while (fgets(str, MAX_STR_LEN, fp2) != NULL) {
                fputs(str, fp1);
        }
	return SLURM_SUCCESS;
}

int _merge_files(int n_nodes)
{
	int i;
	FILE *part_fp = NULL;
	debug2("in _merge_files");	
	
	for(i = 0; i < n_nodes; i++) {
		sprintf(trace_body_with_id, "%s_%d",trace_body, i);	
		part_fp = fopen(trace_body_with_id, "r");
		if (part_fp != NULL) {
			if(_merge_in_file(trace_fp, part_fp) != SLURM_SUCCESS)
				return SLURM_ERROR;
			fclose(part_fp);
			remove(trace_body_with_id);
		}
		else return SLURM_ERROR;
	}
	return SLURM_SUCCESS;
}

/* Slurmctld complete at the end of the execution the first line of 
 * paraver prv file
 */
int slurmctld_extrae_trace_fini(struct node_record *node_table, int node_record_count)
{
	struct timeval fini_time;
	long elapsed;
	int i, j, first, end;
	ListIterator itr = NULL;
	extrae_job_t *job_ptr = NULL;

	debug("In slurmctld_extrae_trace_fini");
	
	gettimeofday(&fini_time, NULL);

	slurm_mutex_lock(&extrae_lock);

	if (!trace_initialized) {
		slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
	}
//	fseek(trace_fp, trace_prv_header_offset, SEEK_SET);

	elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;	

	fprintf(trace_fp, "%ld:", elapsed);
	fprintf(trace_fp, "%d(", node_record_count);
	/* print cpus per each node */
	for(i = 0; i < node_record_count - 1; i++) {
		fprintf(trace_fp, "%d,", node_table[i].cpus);
	}
	fprintf(trace_fp, "%d):%d", node_table[i].cpus, list_count(extrae_job_list));
	/* print app list */
	itr = list_iterator_create(extrae_job_list);	
	while((job_ptr = list_next(itr))) {
		//ntasks
		first = 1;
		fprintf(trace_fp, ":%d(", job_ptr->num_tasks);
		end = bit_fls(job_ptr->node_bitmap);
		for(i = bit_ffs(job_ptr->node_bitmap); i <= end; i++) {
			if (!bit_test(job_ptr->node_bitmap, i))
                                        continue;
			for(j = 0; j < job_ptr->ntasks_per_node; j++)
				//threads for each task
				if(first) {
					fprintf(trace_fp, "%d:%d", job_ptr->cpus_per_task, i + 1);
					first = 0;
				}
				else
					fprintf(trace_fp, ",%d:%d", job_ptr->cpus_per_task, i + 1);
			}
		fprintf(trace_fp, ")");
	}
	fprintf(trace_fp, "\n");
	
	fclose(body_fp);
	body_fp = fopen(trace_body_slurmctld, "r");
	_merge_in_file(trace_fp, body_fp);
	fclose(body_fp);
	remove(trace_body_slurmctld);

	_merge_files(node_record_count);
	
	fflush(trace_fp);
	fclose(trace_fp);
	remove(time_file);
	list_destroy(extrae_job_list);
	slurm_mutex_unlock(&extrae_lock);
	
	return SLURM_SUCCESS;
}

void slurmctld_extrae_add_job_to_queue(struct job_record *job_ptr)
{
	struct timeval fini_time;

	debug("In slurmctld_extrae_add_job_to_queue");

        gettimeofday(&fini_time, NULL);
	extrae_job_t *new_job = xmalloc(sizeof(extrae_thread_t));
	new_job->job_id = job_ptr->job_id;
	new_job->arrival_time = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;
	//job_ptr->node_bitmap	
	list_append(extrae_job_list, new_job);
}

int find_job_per_id(void *x, void *key)
{
	extrae_job_t *job = (extrae_job_t *) x;
	int *jobid = (int *) key;
	if (job->job_id == *jobid)
		return 1;
	return 0;
}

//int slurmctld_extrae_start_job(struct job_record *job_ptr, struct node_record *node_table)
int slurmctld_extrae_start_job(struct job_record *job_ptr)
{
	struct timeval fini_time;
	long elapsed;
	int node_count;
//	int i, first_cpu, node_count;
	debug("In slurmctld_extrae_start_job");

	extrae_job_t *job = list_find_first(extrae_job_list, find_job_per_id , (void *)&job_ptr->job_id);
	if (job == NULL) {
		debug("job not found");
		return SLURM_ERROR;
	}
	gettimeofday(&fini_time, NULL);

	job->cpus_per_task = job_ptr->details->cpus_per_task;
	job->num_tasks = job_ptr->details->num_tasks;
	job->ntasks_per_node = job_ptr->details->ntasks_per_node;
	job->node_bitmap = bit_copy(job_ptr->node_bitmap);
	
	/* I need tasks distribution infos: at least ntasks_per_node */
	//TODO: manage those informations at job arrival
	node_count = bit_set_count(job->node_bitmap);
	if (job->num_tasks == 0 && job->ntasks_per_node != 0)
		job->num_tasks = node_count * job->ntasks_per_node;
	else if (job->num_tasks == 0 && job->ntasks_per_node == 0) //1 task per node
		job->num_tasks = node_count;
	if (job->ntasks_per_node == 0)
		job->ntasks_per_node = node_count / job->num_tasks;
//	debug("Node count %d ", node_count);
//	for(i = 0; i < node_count; i++)
//		if(bit_test(job->node_bitmap, i))
//			first_cpu = i * node_table[i].cpus + 1;

	elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;	
//	fprintf(body_fp, "1:%d:%d:1:1:%ld:%ld:%d\n",first_cpu, job->job_id - first_job, job->arrival_time, elapsed, WAITING);
	fprintf(body_fp, "1:1:%d:1:1:%ld:%ld:%d\n", job->job_id - first_job, job->arrival_time, elapsed, WAITING);
	return SLURM_SUCCESS;
}

/* SLURMD functions */

int slurmd_extrae_trace_init(int ncpus, char *node_name)
{
        int i;
	FILE *time_fp;
	node_info_msg_t *node_info_ptr = NULL;
	debug("In slurmd_extrae_trace_init");

	if(trace_initialized)
		return SLURM_SUCCESS;

	slurm_ctl_conf_t *conf = slurm_conf_lock();
        first_job = conf->first_job_id;
        slurm_conf_unlock();

//        gettimeofday(&init_time, NULL);
  	time_fp = fopen(time_file, "r");
	if(time_fp == NULL)
                return SLURM_ERROR;
	fread(&init_time, sizeof(init_time), 1, time_fp);
 	fclose(time_fp);
	if (!ncpus) {
		debug("Number of CPUs not defined");
		return SLURM_ERROR;
	}
	else debug("Number of CPUs: %d", ncpus);
	n_cpus = ncpus;
	extrae_threads = xmalloc(sizeof(extrae_thread_t) * n_cpus);
        for(i = 0; i < n_cpus; i++) {
                extrae_threads[i].job_id = -1;
	}
	if (slurm_load_node((time_t) NULL, &node_info_ptr, SHOW_ALL) ) {
		debug("slurm_load_node error");
		return SLURM_ERROR;
	}
	for (i = 0; i < node_info_ptr->record_count; i++)
		if(strcmp(node_name, node_info_ptr->node_array[i].name) == 0)
			node_id = i;
	slurm_free_node_info_msg(node_info_ptr);
	if (node_id == -1)
		return SLURM_ERROR;
	base_cpu_id = node_id * n_cpus;
        sprintf(trace_body_with_id, "%s_%d",trace_body, node_id);
        //create the file
        body_fp = fopen(trace_body_with_id,"w");
        if (body_fp == NULL)
        	return SLURM_ERROR;

	trace_initialized = 1;
        return SLURM_SUCCESS;
}

int slurmd_extrae_trace_fini()
{
	debug("In slurmd_extrae_trace_fini");
	fclose(body_fp);
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
	struct timeval fini_time;
	long elapsed;

//	debug("_start_thread\n");
	
        gettimeofday(&fini_time, NULL);
        elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;
        
	sprintf(extrae_threads[cpu_id].entry, "1:%d:%d:%d:%d:%ld", cpu_id + 1 + base_cpu_id, app_id, task_id, th_id, elapsed);
}

static int _stop_thread(int cpu_id)
{
        struct timeval fini_time;
	int len;
	long elapsed;

//	debug("In _stop_thread\n");

        gettimeofday(&fini_time, NULL);
        elapsed = (fini_time.tv_sec - init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;
	len = strlen(extrae_threads[cpu_id].entry);
	sprintf(extrae_threads[cpu_id].entry + len, ":%ld:%d", elapsed, RUNNING);

        slurm_mutex_lock(&extrae_lock);

//        trace_fp = fopen(trace_body_with_id,"a");
//        if (trace_fp == NULL) {
//                slurm_mutex_unlock(&extrae_lock);
//                return SLURM_ERROR;
//        }
        fprintf(body_fp, "%s\n", extrae_threads[cpu_id].entry);
	extrae_threads[cpu_id].job_id = -1;
	fflush(body_fp);
//	fclose(trace_fp);

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
	int app_id;
	debug2("In slurmd_extrae_start_thread\n");
	if (slurmd_extrae_stop_thread(cpu_id) != SLURM_SUCCESS) {
		debug("Error in slurmd_extrae_stop_thread");
		return SLURM_ERROR;
	}
	
	app_id = job_id - first_job;
	_start_thread(cpu_id, app_id, task_id, th_id);
	extrae_threads[cpu_id].job_id = job_id;
	extrae_threads[cpu_id].task_id = task_id;
	extrae_threads[cpu_id].thread_id = th_id;
	//_print_extrae_threads();
	return SLURM_SUCCESS;
}

int slurmd_extrae_stop_thread(int cpu_id)
{
	debug2("In slurmd_extrae_stop_thread\n");

	if(extrae_threads[cpu_id].job_id == -1)
		return SLURM_SUCCESS;
	if(_stop_thread(cpu_id) != SLURM_SUCCESS)
		return SLURM_ERROR;
	extrae_threads[cpu_id].job_id = -1;
	//_print_extrae_threads();
	return SLURM_SUCCESS;
}
