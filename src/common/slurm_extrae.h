#ifndef _SLURM_EXTRAE_H
#define _SLURM_EXTRAE_H

#include "slurm/slurm.h"

#define	IDLE 		0
#define	RUNNING 	1
#define	NOT_CREATED 	2

#define EXTRAE_STRING_LEN 150

typedef struct extrae_thread {
//	int app_id;
	int job_id;
	char entry[EXTRAE_STRING_LEN];
//	int cpu_id;
	int task_id;
	int thread_id;
} extrae_thread_t;

int slurmctld_extrae_trace_init();
int slurmctld_extrae_trace_fini();
int slurmd_extrae_trace_init(int ncpus);
int slurmd_extrae_trace_fini();

int slurmd_get_next_extrae_thread(int job_id, int task_id);

int slurmd_extrae_start_thread(int job_id, int cpu_id, int task_id, int th_id);
int slurmd_extrae_stop_thread(int cpu_id);

#endif
