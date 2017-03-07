#ifndef _SLURMSTEPD_DIST_TASKS_H
#define _SLURMSTEPD_DIST_TASKS_H

#define MAX_PROCS 64

typedef struct cpu_steal_info {
        int             job_id;
        int             cpu_bind_type;          //needed for resource redistribution
        int             task_dist;              //needed for resource redistribution
        int             ntasks;                 //number of task in the node for the job
                                                //equal to number of masks in manual masks
        cpu_set_t       *manual_masks;          //mask assigned by user, always respect them
                                                //when expanding a job, if not present,
                                                // we need to create a new distribution
        cpu_set_t       *auto_mask;             //final general mask for job (not single processes)
                                                //used in non manual case
        cpu_set_t       *general_mask;          //slurmctld mask, used when expanding job
                                                //to retrive correct cpus
        cpu_set_t       *assigned_mask;         //assigned masks for processes
        int             original_cpt;
        int             assigned_cpt;
        int             nsteals;                //number of stealing done for this job
        int             got_stolen;
        cpu_set_t       **stolen;               //list of stolen cpus
} cpu_steal_info_t;

extern int32_t ncpus;

int DLB_Drom_steal_cpus(launch_tasks_request_msg_t *req, uint32_t node_id);
int DLB_Drom_Register_task(job, cur_mask);
int DLB_Drom_reassign_cpus(uint32_t job_id);
#endif
