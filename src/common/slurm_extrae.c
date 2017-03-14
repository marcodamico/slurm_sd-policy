#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "src/common/slurm_extrae.h"
#include "src/common/list.h"
//this lock is between threads, I should implement a inter-process lock
static pthread_mutex_t extrae_lock = PTHREAD_MUTEX_INITIALIZER;

timeval init_time;
int trace_initialized = 0;
char *trace_prv = "slurm_workload_trace.prv"
char *trace_pcf = "slurm_workload_trace.pcf"
char *trace_row = "slurm_workload_trace.row"
off_t trace_prv_header_offset = 0;

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

	fprintf(trace_fp, "#PARAVER (%d/%d/%d at %d:%d):", tm_now.tm_mday, tm_now.tm_mon, tm_now.tm_year, tm_now.tm_hour, tm_now.tm_min);
	gettimeofday(&init_time, NULL);
	trace_prv_header_offset = ftell(trace_fp);
	//use second line to store init_time, as start time in header is limited to hh:mm
	fprintf(trace_fp, "\n%d %d", init_time.tv_sec, init_time.tv_usec);
	fclose(trace_fp);
	trace_initialized = 1;
	slurm_mutex_unlock(&extrae_lock);
	return 0;
}

int slurmd_extrae_trace_init()
{
	gettimeofday(&init_time, NULL);
	return 0;
}

/* Slurmctld complete at the end of the execution the first line of 
 * paraver prv file
 */
int slurmctld_extrae_trace_fini(List job_list, List node_list, )
{
	timeval fini_time;
	FILE *trace_fp;
	long elapsed;
	
	gettimeofday(&fini_time, NULL);

	slurm_mutex_lock(&extrae_lock);

	if (!trace_initialized) {
		slurm_mutex_unlock(&extrae_lock);
                return SLURM_ERROR;
	}
	trace_fp = fopen(trace_prv,"w+");
	fseek(trace_fp, trace_prv_header_offset, SEEK_SET);

	elapsed = (fini_time.tv_sec-init_time.tv_sec) * 1000000 + fini_time.tv_usec - init_time.tv_usec;	

	fprintf(trace_fp, "%d:", elapsed);
	fprintf(trace_fp, "%d(", list_count(node_list));
	/* print cpus per each node */	 
	while() {

	}
	fprintf(trace_fp, "):%d:", list_count(node_list));
	/*print app list*/
	while() {
		//ntasks
		fprintf(trace_fp, "%d(", );
		for()
			//threads for each node
			fprintf(trace_fp, "%d:%d,",);
		//not for last line
        	fprintf(trace_fp, "):");

		fprintf(trace_fp, ")");
	}
	fclose(trace_fp);
}

void slurmd_start_thread(char *entry, int cpu_id, app_id, th_id, int time)
{
	sprintf(entry, "%d:%d:%d:%d:%d", cpu_id, app_id, th_id, time);
}

void slurmd_stop_thread(char *entry, int time)
{
	sprintf(entry, ":%d:%d",time, RUNNING);
}

void slurmd_change_cpu_owner(char *from, char *to, int cpu_id, app_id, th_id, int time)
{
	slurmd_stop_thread(from, time);
	slurmd_start_thread(to, int cpu_id, app_id, th_id, int time);
}
