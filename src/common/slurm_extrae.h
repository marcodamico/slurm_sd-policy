#ifndef _SLURM_EXTRAE_H
#define _SLURM_EXTRAE_H

#include "slurm/slurm.h"
#include "src/slurmctld/slurmctld.h"

#define	IDLE 		0
#define	RUNNING 	1
#define	NOT_CREATED 	2
#define WAITING		8
#define EXTRAE_STRING_LEN 150

typedef struct extrae_thread {
//	int app_id;
	int job_id;
	char entry[EXTRAE_STRING_LEN];
//	int cpu_id;
	int task_id;				//global task id
	int thread_id;
} extrae_thread_t;

typedef struct extrae_job {
	int job_id;
        int cpus_per_task;                            //global task id
	int num_tasks;
	int ntasks_per_node;
	bitstr_t *node_bitmap;
	long arrival_time;
} extrae_job_t;

/* TODO: find a better way for writing header of the trace */
int slurmctld_extrae_trace_init();
int slurmctld_extrae_trace_fini(struct node_record *node_table, int node_record_count);

void slurmctld_extrae_add_job_to_queue(struct job_record *job_ptr);
int slurmctld_extrae_start_job(struct job_record *job_ptr);

int slurmd_extrae_trace_init(int ncpus);
int slurmd_extrae_trace_fini();

/* find the next thread by searching in the thread list for that 
 * specific app and task used when not starting threads in order */
int slurmd_get_next_extrae_thread(int job_id, int task_id);

/* node_id is important only at first call of this function
 * TODO: find an alternative way to do it in slurmd_extrae_trace_init */
int slurmd_extrae_start_thread(int job_id, int cpu_id, int task_id, int th_id, int node_id);

int slurmd_extrae_stop_thread(int cpu_id);

#endif
