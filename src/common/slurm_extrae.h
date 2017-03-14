#ifndef _SLURM_EXTRAE_H
#define _SLURM_EXTRAE_H

#define	IDLE 		= 0,
#define	RUNNING 	= 1,
#define	NOT_CREATED 	= 2


int slurmctld_extrae_trace_init();
int slurmctld_extrae_trace_fini();

void slurmd_start_thread(char *entry, int cpu_id, app_id, th_id, int time);
void slurmd_stop_thread(char *entry, int time);
void slurmd_change_cpu_owner(char *from, char *to, int cpu_id, app_id, th_id, int time);

#endif
