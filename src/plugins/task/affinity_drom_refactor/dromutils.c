#include "DLB_interface.h"

#include <sys/time.h> //for timing

#include <time.h> //for timing
#include <sys/time.h> //for timing

#include "src/common/xmalloc.h"

#include "dromutils.h"
#include "DLB_interface.h"

cpu_steal_info_t *cpu_steal_infos[MAX_PROCS];
uint32_t n_active_jobs = 0;
uint32_t n_active_procs = 0;
int32_t ncpus;
double  DLB_share_factor = 0;

#define DROM_SYNC_WAIT_USECS 100

void cpu_steal_info_init(cpu_steal_info_t **infos) {
        int i;

        *infos = (cpu_steal_info_t *) xmalloc(sizeof(cpu_steal_info_t));
        (*infos)->job_id = -1;
        (*infos)->cpu_bind_type = -1;
        (*infos)->task_dist = -1;
        (*infos)->ntasks = 0;
        (*infos)->manual_masks = NULL;
        (*infos)->auto_mask = NULL;
        (*infos)->general_mask = NULL;
        (*infos)->assigned_mask = NULL;
        (*infos)->original_cpt = 0;
        (*infos)->assigned_cpt = 0;
        (*infos)->nsteals = 0;
        (*infos)->got_stolen = 0;
        (*infos)->stolen = (cpu_set_t **) xmalloc(sizeof(cpu_set_t *) * ncpus);
        for(i = 0; i < ncpus; i++)
                (*infos)->stolen[i] = NULL;
}

void cpu_steal_info_destroy(cpu_steal_info_t *infos) {
        debug("destroying infos of %d",infos->job_id);
        xfree(infos->manual_masks);
        xfree(infos->auto_mask);
        xfree(infos->general_mask);
        xfree(infos->assigned_mask);
        xfree(infos->stolen);
        xfree(infos);
}

void print_cpu_steal_info(cpu_steal_info_t *infos) {
        char str[CPU_SETSIZE+1];
        int i;
        debug("jobid: %d, ntasks: %d, assigned cpt: %d, original cpt: %d, nsteals: %d", infos->job_id, infos->ntasks, infos->assigned_cpt, infos->original_cpt, infos->nsteals);
        cpuset_to_str(infos->general_mask, str);
        debug("general mask: %s",str);
        if(infos->manual_masks != NULL)
                for(i = 0; i < infos->ntasks; i++) {
                        cpuset_to_str(&infos->manual_masks[i], str);
                        debug("manual mask: %s",str);
                }
        if(infos->assigned_mask)
                for(i = 0; i < infos->ntasks; i++) {
                        cpuset_to_str(&infos->assigned_mask[i], str);
                        debug("assigned mask: %s",str);
                }
        if(infos->auto_mask)
                cpuset_to_str(infos->auto_mask, str);
        debug("auto mask: %s",str);
        for(i = 0; i < ncpus; i++)
                if(infos->stolen[i])
                        debug("cpu %d stolen",i);
}

int get_DLB_procs_masks(int *dlb_pids, cpu_set_t **dlb_masks, int *npids) {
        int j;
        cpu_set_t *masks;
        debug("In get_DLB_procs_masks");
        DLB_Drom_GetPidList(dlb_pids, npids, MAX_PROCS);
        debug("Found %d processes to steal from", *npids);
        if(*npids == 0) {
                masks = NULL;
                return SLURM_SUCCESS;
        }
        masks = (cpu_set_t *) xmalloc(sizeof(cpu_set_t)*(*npids));

        for(j=0;j<*npids;j++) {
                if(DLB_Drom_GetProcessMask(dlb_pids[j],(dlb_cpu_set_t) &masks[j])) {
                        debug("Failed to get DROM process mask of %d",dlb_pids[j]);
                        return SLURM_ERROR;
                }
        }
        *dlb_masks = masks;
        return SLURM_SUCCESS;
}

void get_conflict_masks(cpu_set_t *req_set, int nsets, cpu_set_t *conflict_mask) {
        int i, j, k;
        cpu_set_t mask;
        debug("In get_conflict_masks");

        CPU_ZERO(conflict_mask);
        for(i = 0; i < nsets; i++)
                for(j = 0; j < n_active_jobs; j++)
                        for(k = 0; k < cpu_steal_infos[j]->ntasks; k++) {
                                //AND to get CPUs in common between DLB processes and new Job
                                CPU_AND(&mask, &req_set[i], &cpu_steal_infos[j]->assigned_mask[k]);
                                //OR to add it to conflict mask
                                CPU_OR(conflict_mask, &mask, conflict_mask);
                        }
}

int steal_cpus_from_proc(int index, int proc_index, cpu_steal_info_t *steal_infos, cpu_set_t *steal_mask, cpu_set_t *final_mask, cpu_set_t *conflict_mask, int to_steal) {
        int i, k, stolen = 0;
        cpu_steal_info_t *victim = cpu_steal_infos[index];
        char str[CPU_SETSIZE+1];
        debug("In steal_cpus_from_proc");
        //try to give consecutive cpus TODO:implement some isolated first policy? (give isolated cpus first) (or steal from less occupied socket first (if to_steal is low enough)
        for(i = ncpus - 1; i >= 0; i--) {
                cpuset_to_str(steal_mask, str);
                debug("steal_mask: %s", str);
                if(stolen == to_steal) {
                        debug("got %d CPUs, exiting steal_cpus_from_proc", stolen);
                        break;
                }
                if(!CPU_ISSET(i, steal_mask))
                        continue;
                debug("Trying to steal CPU %d",i);
                if(victim->stolen[i]) {
                        debug("This CPU got stolen from another process, skipping it");
                        continue;
                }
                //if already have a process distribution check we balance steal
                if(steal_infos->manual_masks != NULL) {
                        for(k = 0; k < steal_infos->ntasks; k++) {
                                if(CPU_ISSET(i, &steal_infos->manual_masks[k])) {
                                        if(CPU_COUNT(&final_mask[k]) >= steal_infos->assigned_cpt) {
                                                debug("This task already has all assigned cpus");
                                                debug("ncpus %d",CPU_COUNT(&final_mask[k]));
                                        }
                                        else {
                                                debug("stolen cpu %d from step %d.%d", i, cpu_steal_infos[index]->job_id, index);
                                                CPU_SET(i, &final_mask[k]);
                                                CPU_CLR(i, &victim->assigned_mask[proc_index]);
                                                CPU_CLR(i, conflict_mask);
                                                stolen++;
                                                steal_infos->stolen[i] = &victim->assigned_mask[proc_index];
                                        }
                                        break;
                                }
                        }
                }
                else {
                        debug("stolen cpu %d from step %d.%d", i, cpu_steal_infos[index]->job_id, index);
                        CPU_SET(i, final_mask);
                        CPU_CLR(i, conflict_mask);
                        CPU_CLR(i, &victim->assigned_mask[proc_index]);
                        CPU_CLR(i, victim->auto_mask);
                        stolen++;
                        steal_infos->stolen[i] = &victim->assigned_mask[proc_index];
                }
        }
        return stolen;
}

//TODO: serve un algoritmo di selezione CPUs migliore
void sort_by_values(int *vector, float *values, int n) {
        int i,j;
        float tmp;

        for (i = 0; i < n -1; i++)
                for (j = 0; j< n-i-1; j++)
                        if (values[vector[j]] > values[vector[j+1]]) {
                                tmp = vector[j];
                                vector[j] = vector[j+1];
                                vector[j+1] = tmp;
                        }
        for (i = 0; i < n; i++)
                debug("proc %d: %f", vector[i], values[vector[i]]);
}

List steal_cpus(cpu_steal_info_t *steal_infos, int final_steal, cpu_set_t *conflict_mask, cpu_set_t *non_free_mask, cpu_set_t *final_mask) {
        int stolen, tot_stolen = 0, to_steal, i, j;
        cpu_set_t steal_mask;
        float *values;
        int  *ordered, *max_steal, nordered;
        int max_steal_per_task;
        List job_dependencies = list_create(NULL);
        values = xmalloc(sizeof(float) * n_active_procs);
        ordered = xmalloc(sizeof(int) * n_active_procs);
        max_steal = xmalloc(sizeof(int) * n_active_procs);
        nordered = n_active_procs;
        char str[CPU_SETSIZE+1];

        debug("In steal_cpus");
        for(j = 0; j < n_active_jobs; j++) {
                values[j] = (float) final_steal * cpu_steal_infos[j]->original_cpt * cpu_steal_infos[j]->ntasks  / CPU_COUNT(non_free_mask);
                max_steal[j] =(int) ((float)(cpu_steal_infos[j]->original_cpt * cpu_steal_infos[j]->ntasks) * DLB_share_factor + 0.5f);
                debug("value %f max steal %d", values[j], max_steal[j]);
                print_cpu_steal_info(cpu_steal_infos[j]);
                if(max_steal[j] - cpu_steal_infos[j]->got_stolen - cpu_steal_infos[j]->nsteals < values[j])
                        values[j] = max_steal[j] - cpu_steal_infos[j]->got_stolen - cpu_steal_infos[j]->nsteals;
                ordered[j] = j;
        }
        sort_by_values(ordered,values,nordered);
        while(tot_stolen < final_steal && nordered > 0) {
                j = ordered[nordered-1];
                max_steal_per_task = max_steal[j] / cpu_steal_infos[j]->ntasks;
                for(i = 0; i < cpu_steal_infos[j]->ntasks && tot_stolen < final_steal; i++) {
                        //TODO:we can steal first on one socket, than on second, to group steal, we simply need to filter conflict mask
                        //TODO:bring this out, we are at task level here, not job level, 
                        //     max_steal got stolen and nsteals are at job level!!!
                        CPU_AND(&steal_mask, &cpu_steal_infos[j]->assigned_mask[i], conflict_mask);
                        cpuset_to_str(&steal_mask, str);
                        debug("steal_mask: %s", str);
                        cpuset_to_str(&cpu_steal_infos[j]->assigned_mask[i], str);
                        debug("assigned_mask: %s", str);
                        cpuset_to_str(conflict_mask, str);
                        debug("conflict_mask: %s", str);
                        to_steal = values[j] > 1 ? values[j] : 1;

                        if(to_steal > max_steal_per_task);
                                to_steal = max_steal_per_task;
                        //TODO:bring this out, we are at task level here, not job level, 
                        //     max_steal got stolen and nsteals are at job level!!!
                        if(to_steal + cpu_steal_infos[j]->got_stolen + cpu_steal_infos[j]->nsteals > max_steal[j])
                                to_steal = max_steal[j] - cpu_steal_infos[j]->got_stolen - cpu_steal_infos[j]->nsteals;
                        if(to_steal == CPU_COUNT(&cpu_steal_infos[j]->assigned_mask[i])) {
                                debug("Trying to steal all cpus from proc %d.%d", cpu_steal_infos[j]->job_id, i);
                                to_steal--;
                        }

                        if((to_steal + tot_stolen) > final_steal)
                                to_steal = final_steal - tot_stolen;

                        debug("requested %d cpus already in use, stealing %d from step %d.%d, value: %f", final_steal, to_steal, cpu_steal_infos[j]->job_id, i, values[j]);
                        stolen = steal_cpus_from_proc(j, i, steal_infos, &steal_mask, final_mask, conflict_mask, to_steal);
                        if(stolen > 0) {
                                tot_stolen += stolen;
                                cpu_steal_infos[j]->got_stolen += stolen;
                                values[j] -= stolen;
                                list_append(job_dependencies, &cpu_steal_infos[j]->job_id);
                        }
                        else {
                                values[j] = -1;
                                nordered--;
                        }
                        sort_by_values(ordered,values,nordered);
                }
        }
        xfree(values);
        xfree(ordered);
        xfree(max_steal);

        steal_infos->nsteals = tot_stolen;
        return job_dependencies;
}

int get_final_steal(int *cpus_per_task, cpu_set_t *non_free_mask, int tot_steal, int tasks_to_launch) {
        int max_steal, final_steal;
        max_steal = (float) CPU_COUNT(non_free_mask) * DLB_share_factor + 0.5;
        final_steal = tot_steal;

        while(final_steal > max_steal) {
                *cpus_per_task = *cpus_per_task - 1;
                //remove one cpu per task, to keep it balanced
                final_steal -= tasks_to_launch;
        }

        debug("Stealing %d cpus, Max steals %d, final steal %d",tot_steal, max_steal, final_steal);
        return final_steal;
}

void get_non_free_mask(cpu_set_t *non_free_mask) {
        int i, j;

        CPU_ZERO(non_free_mask);
        for(i = 0; i < n_active_jobs; i++) {
                for(j = 0; j < cpu_steal_infos[i]->ntasks; j++)
                        CPU_OR(non_free_mask, non_free_mask, &cpu_steal_infos[i]->assigned_mask[j]);
        }
        char mask_str[1 + CPU_SETSIZE / 4];
        cpuset_to_str(non_free_mask, mask_str);
        debug("Non free mask: %s", mask_str);
}

int get_necessary_masks(launch_tasks_request_msg_t *req, uint32_t node_id, cpu_steal_info_t *infos, cpu_set_t *conflict_mask, cpu_set_t **final_mask, cpu_set_t *non_free_mask) {
        char *tok, *save_ptr = NULL;
        int i, nsets;
        cpu_set_t *req_set, non_conflict_mask;
        static uint16_t bind_mode = CPU_BIND_NONE   | CPU_BIND_MASK   |
                                    CPU_BIND_RANK   | CPU_BIND_MAP    |
                                    CPU_BIND_LDMASK | CPU_BIND_LDRANK |
                                    CPU_BIND_LDMAP;

        int whole_nodes, whole_sockets, whole_cores, whole_threads;
        int part_sockets, part_cores;
        //save in general_mask slurmctld mask
        char *avail_mask = _alloc_mask(req,
                                       &whole_nodes,  &whole_sockets,
                                       &whole_cores,  &whole_threads,
                                       &part_sockets, &part_cores);

        debug("got avail mask %s", avail_mask);
        infos->general_mask = xmalloc(sizeof(cpu_set_t));
        CPU_ZERO(infos->general_mask);
        str_to_cpuset(infos->general_mask, avail_mask);
        xfree(avail_mask);

        //get mask specified by user if available
        if (req->cpu_bind_type & bind_mode) {
                debug("Specified manual mask for job %d", req->job_id);
                infos->manual_masks = xmalloc(sizeof(cpu_set_t)*req->tasks_to_launch[node_id]);
                req_set = infos->manual_masks;
                //convert validated manual mask to vector of cpu_set_t 
                //TODO: this works only if mask is specified??
                tok = strtok_r(req->cpu_bind, ",", &save_ptr);
                i = 0;
                while(tok) {
                        debug("mask %d: %s", i, tok);
                        CPU_ZERO(&req_set[i]);
                        (void) str_to_cpuset(&req_set[i], tok);
                        tok = strtok_r(NULL, ",", &save_ptr);
                        i++;
                }

                if(i != req->tasks_to_launch[node_id])
                        debug("number of masks different than number of tasks?");
                nsets = i;
        }
        else {
                debug("Mask selected by plugin for job %d", req->job_id);
                req_set = infos->general_mask;
                nsets = 1;
        }

        get_non_free_mask(non_free_mask);

        *final_mask = (cpu_set_t *) xmalloc(sizeof(cpu_set_t) * nsets);

        //get conflict mask per task and return total necessary steals
        get_conflict_masks(req_set, nsets, conflict_mask);

        CPU_ZERO(&non_conflict_mask);
        for(i = 0;i < ncpus; i++)
                if(!CPU_ISSET(i,conflict_mask))
                        CPU_SET(i, &non_conflict_mask);

        //init final mask with free cpus
        for(i = 0; i < nsets; i++) {
                CPU_AND(final_mask[i], &non_conflict_mask, &req_set[i]);
        }

        return SLURM_SUCCESS;
}

nt DLB_Drom_steal_cpus(launch_tasks_request_msg_t *req, uint32_t node_id) {
        int tot_steal, final_steal, cpus_per_task = 0;
        cpu_set_t conflict_mask, *final_mask = NULL, non_free_mask;
        char *new_mask = NULL, mask_str[1 + CPU_SETSIZE / 4];
        int i, nsets;
        cpu_steal_info_t *steal_infos;
//      partition_info_msg_t *part_ptr = NULL;
        slurm_ctl_conf_t *config;

        debug("In DLB_Drom_steal_cpus");

        debug("n active jobs: %d",n_active_jobs);
        //TODO:make SF dynamic?
        config = slurm_conf_lock();
        DLB_share_factor = config->sharing_factor;
        slurm_conf_unlock();
        debug("share factor: %lf", DLB_share_factor);
//      slurm_free_partition_info_msg(part_ptr);
        cpu_steal_info_init(&steal_infos);

        get_necessary_masks(req, node_id, steal_infos, &conflict_mask, &final_mask, &non_free_mask);
        //TODO:how do i manage non DLB jobs? ncpus - CPU_COUNT(non_free_mask) would not be exact
        tot_steal = req->cpus_per_task * req->tasks_to_launch[node_id] - (ncpus - CPU_COUNT(&non_free_mask));
        if(tot_steal < 0)
                tot_steal = 0;
        cpus_per_task = req->cpus_per_task;

        final_steal = get_final_steal(&cpus_per_task, &non_free_mask, tot_steal, req->tasks_to_launch[node_id]);

        if( (final_steal + CPU_COUNT(steal_infos->general_mask) - CPU_COUNT(&conflict_mask)) < req->tasks_to_launch[node_id]) {
                error("Not stealing enough resource to start tasks with at least on cpu per task");
                xfree(final_mask);
                cpu_steal_info_destroy(steal_infos);
                return SLURM_ERROR;
        }

        steal_infos->ntasks = req->tasks_to_launch[node_id];
        steal_infos->job_id = req->job_id;
        steal_infos->cpu_bind_type = req->cpu_bind_type;
        steal_infos->task_dist = req->task_dist;
        steal_infos->original_cpt = req->cpus_per_task;
        steal_infos->assigned_cpt = cpus_per_task;

        List job_dependencies;

        if(final_steal != 0) {
                job_dependencies = steal_cpus(steal_infos, final_steal, &conflict_mask, &non_free_mask, final_mask);
                req->job_dependencies = job_dependencies;
                debug("Steal complete!");
        }

        //assign new binding and cpus per task that plugin will respect
        if(steal_infos->manual_masks != NULL)
                steal_infos->assigned_mask = final_mask;
        else
                steal_infos->auto_mask = final_mask;

        req->cpus_per_task = cpus_per_task;
        xfree(req->cpu_bind);
        nsets = steal_infos->auto_mask == NULL ? steal_infos->ntasks : 1;
        //create string to update req
        for(i = 0; i < nsets; i++) {
                cpuset_to_str(&final_mask[i], mask_str);
                if (new_mask)
                        xstrcat(new_mask, ",");
                xstrcat(new_mask, mask_str);
        }
        //This is a sync point, in order to do a correct stealing
        //XXX:this is replaced by step_connect across steps
//      if(steal_infos->nsteals != 0)
//                wait_for_dependent_processes();

        //store in cpu_steal_info vector
        cpu_steal_infos[n_active_jobs] = steal_infos;
        print_cpu_steal_info(cpu_steal_infos[n_active_jobs]);
        n_active_jobs++;
        n_active_procs += steal_infos->ntasks;
        debug("n_active_jobs: %d", n_active_jobs);
        req->cpu_bind = new_mask;

	return SLURM_SUCCESS;
}

nt DLB_Drom_return_to_owner(cpu_steal_info_t *to_destroy, cpu_set_t * nonfreemask) {
        int i,j,k;

        debug("In DLB_Drom_return_to_owner");

        for(i = 0; i < ncpus; i++) {
                if(to_destroy->stolen[i] == NULL)
                        continue;

                CPU_SET(i, to_destroy->stolen[i]);
                CPU_SET(i, nonfreemask);
                //TODO:maybe save this info
                for(j = 0; j < n_active_jobs; j++)
                        for(k = 0; k < cpu_steal_infos[j]->ntasks; k++)
                                if(CPU_EQUAL(to_destroy->stolen[i], &cpu_steal_infos[j]->assigned_mask[k])) {
                                        cpu_steal_infos[j]->got_stolen--;
                }
        }

        return SLURM_SUCCESS;
}

int distribute_resources(int index, bitstr_t ***masks_p) {
        int last_taskcount = -1, taskcount = 0;
        uint16_t i, s, hw_sockets = 0, hw_cores = 0, hw_threads = 0;
        uint16_t offset = 0, p = 0;
        int size, max_tasks = cpu_steal_infos[index]->ntasks;
        int max_cpus = max_tasks * cpu_steal_infos[index]->assigned_cpt; //update this value??
        bitstr_t *avail_map;
        bitstr_t **masks = NULL;
        int *socket_last_pu = NULL, depth;
        hwloc_topology_t topology;
        hwloc_topology_init(&topology);
        hwloc_topology_load(topology);

        debug("In distribute_resources");

        avail_map = bit_alloc(ncpus);

        cpuset_to_bitstr(avail_map, cpu_steal_infos[index]->auto_mask, ncpus);

        depth = hwloc_get_type_depth(topology, HWLOC_OBJ_SOCKET);
        hw_sockets = hwloc_get_nbobjs_by_depth(topology, depth);
        depth = hwloc_get_type_depth(topology, HWLOC_OBJ_CORE);
        hw_cores = hwloc_get_nbobjs_by_depth(topology, depth);
        depth = hwloc_get_type_depth(topology, HWLOC_OBJ_PU);
        hw_threads = hwloc_get_nbobjs_by_depth(topology, depth) / hw_cores;
        hw_cores = ncpus / hw_sockets;
        debug("sockets %d, cpus %d, threads %d", hw_sockets, hw_cores, hw_threads);

        hwloc_topology_destroy(topology);

        size = bit_set_count(avail_map);
        if (size < max_tasks) {
                error("task/affinity: only %d bits in avail_map for %d tasks!",
                      size, max_tasks);
                FREE_NULL_BITMAP(avail_map);
                return SLURM_ERROR;
        }
        if (size < max_cpus) {
                /* Possible result of overcommit */
                i = size / max_tasks;
                info("task/affinity: reset cpus_per_task from %d to %d",
                     cpu_steal_infos[index]->assigned_cpt, i);
                cpu_steal_infos[index]->assigned_cpt = i;
        }

        socket_last_pu = xmalloc(hw_sockets * sizeof(int));

        *masks_p = xmalloc(max_tasks * sizeof(bitstr_t*));
        masks = *masks_p;

        size = bit_size(avail_map);

        offset = hw_cores * hw_threads;
        s = 0;
        while (taskcount < max_tasks) {
                if (taskcount == last_taskcount)
                        fatal("_task_layout_lllp_cyclic failure");
                last_taskcount = taskcount;
                for (i = 0; i < size; i++) {
                        bool already_switched = false;
                        uint16_t bit;
                        uint16_t orig_s = s;

                        while (socket_last_pu[s] >= offset) {
                                s = (s + 1) % hw_sockets;
                                if (orig_s == s) {
                                        /* This should rarely happen,
                                         * but is here for sanity sake.
                                         */
                                        debug("allocation is full, "
                                              "oversubscribing");
                                        memset(socket_last_pu, 0,
                                               sizeof(hw_sockets
                                                      * sizeof(int)));
                                }
                        }

                        bit = socket_last_pu[s] + (s * offset);

                        /* In case hardware and config differ */
                        bit %= size;

                        /* set up for the next one */
                        socket_last_pu[s]++;
                        /* skip unrequested threads */
                        if (cpu_steal_infos[index]->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE)
                                socket_last_pu[s] += hw_threads-1;

                        if (!bit_test(avail_map, bit))
                                continue;

                        if (!masks[taskcount])
                                masks[taskcount] =
                                        bit_alloc(conf->block_map_size);
                        //info("setting %d %d", taskcount, bit);
                        bit_set(masks[taskcount], bit);
                        if (!already_switched &&
                            (((cpu_steal_infos[index]->task_dist & SLURM_DIST_STATE_BASE) ==
                             SLURM_DIST_CYCLIC_CFULL) ||
                            ((cpu_steal_infos[index]->task_dist & SLURM_DIST_STATE_BASE) ==
                             SLURM_DIST_BLOCK_CFULL))) {
                                /* This means we are laying out cpus
                                 * within a task cyclically as well. */
                                s = (s + 1) % hw_sockets;
                                already_switched = true;
                        }
                        if (++p < cpu_steal_infos[index]->assigned_cpt)
                                continue;

                        /* Means we are binding to cores so skip the
                           rest of the threads in this one.  If a user
                           requests ntasks-per-core=1 and the
                           cpu_bind=threads this will not be able to
                           work since we don't know how many tasks per
                           core have been allocated.
                        */
                        if (!(cpu_steal_infos[index]->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE)
                            && ((cpu_steal_infos[index]->cpu_bind_type & CPU_BIND_TO_CORES)
                                || (slurm_get_select_type_param() &
                                    CR_ONE_TASK_PER_CORE))) {
                                int threads_not_used;
                                if (cpu_steal_infos[index]->assigned_cpt < hw_threads)
                                        threads_not_used =
                                                hw_threads - cpu_steal_infos[index]->assigned_cpt;
                                else
                                        threads_not_used =
                                                cpu_steal_infos[index]->assigned_cpt % hw_threads;
                                socket_last_pu[s] += threads_not_used;
                        }
                        p = 0;

                        if (!already_switched) {
                                /* Now that we have finished a task, switch to
                                 * the next socket. */
                                s = (s + 1) % hw_sockets;
                        }

                        if (++taskcount >= max_tasks)
                                break;
                }
        }
        //for(s=0;s<max_tasks;s++)
        //      debug("mask %d: %s",s,bit_fmt_hexmask(bit_to_masks[s]));
        /* last step: expand the masks to bind each task
         * to the requested resource */
        //_expand_masks(cpu_steal_infos[index]->cpu_bind_type, max_tasks, masks,
        //              hw_sockets, hw_cores, hw_threads, avail_map);
        //for(s=0;s<max_tasks;s++)
        //        debug("mask %d: %s",s,bit_fmt_hexmask(bit_to_masks[s]));
        FREE_NULL_BITMAP(avail_map);
        xfree(socket_last_pu);

        return SLURM_SUCCESS;

}

int DLB_Drom_expand_jobs(cpu_set_t *non_free_mask) {
        int i, j, c;
        cpu_set_t free_mask, to_take_mask;
        bitstr_t **new_distribution = NULL;

        CPU_ZERO(&free_mask);
        for(i = 0; i < ncpus; i++)
                if(!CPU_ISSET(i, non_free_mask))
                        CPU_SET(i, &free_mask);

        debug("In DLB_Drom_expand_jobs");
        CPU_ZERO(&to_take_mask);
        for(i = 0; i < n_active_jobs; i++) {
                debug("trying to expand job %d", cpu_steal_infos[i]->job_id);
                if(cpu_steal_infos[i]->manual_masks != NULL) {
                                debug("Manual mask specified");
                                for(j = 0; j < cpu_steal_infos[i]->ntasks; j++) {
                                        //k in pids, j in step pids, i in cpus_steal_infos
                                        //if manual mask case I need to filter each mask with the free available cpus
                                        CPU_OR(&to_take_mask, &cpu_steal_infos[i]->assigned_mask[j], &free_mask);
                                        CPU_AND(&to_take_mask, &to_take_mask, &cpu_steal_infos[i]->manual_masks[j]);

                                        memcpy(&cpu_steal_infos[i]->assigned_mask[j], &to_take_mask, sizeof(cpu_set_t));
                                        //update free and non free mask
                                        CPU_OR(non_free_mask, non_free_mask, &cpu_steal_infos[i]->assigned_mask[j]);
                                        for(c = 0; c < ncpus; c++) {
                                                //make this job the new owner if it was stealer
                                                if( CPU_ISSET(c, &to_take_mask) ) {
                                                        cpu_steal_infos[i]->stolen[c] = NULL;
                                                        cpu_steal_infos[i]->nsteals--;
                                                }
                                                if( !CPU_ISSET(c, non_free_mask) )
                                                        CPU_SET(c, &free_mask);
                                        }
                                }
                }
                else {
                        debug("plugin decided mask case");
                        //char mask[1 + CPU_SETSIZE / 4];
                        //cpuset_to_str(&free_mask, mask);
                        CPU_AND(&to_take_mask, &free_mask, cpu_steal_infos[i]->general_mask);
                        CPU_OR(cpu_steal_infos[i]->auto_mask, &to_take_mask, cpu_steal_infos[i]->auto_mask);

                        int new_cpt = ( CPU_COUNT(cpu_steal_infos[i]->auto_mask) / cpu_steal_infos[i]->ntasks );
                        if(new_cpt > cpu_steal_infos[i]->original_cpt)
                                new_cpt = cpu_steal_infos[i]->original_cpt;
                        if(new_cpt <= cpu_steal_infos[i]->assigned_cpt) {
                                debug("cpt not changed, %d", new_cpt);
                                continue;
                        }
                        cpu_steal_infos[i]->assigned_cpt = new_cpt;
                        debug("new cpt = %d", cpu_steal_infos[i]->assigned_cpt);
                        //generate new distrib
                        if(distribute_resources(i, &new_distribution) != SLURM_SUCCESS) {
                                debug("Error redistributing resources");
                                return SLURM_ERROR;
                        }
                        for(j = 0; j < cpu_steal_infos[i]->ntasks; j++) {
                                CPU_ZERO(&cpu_steal_infos[i]->assigned_mask[j]);
                                bitstr_to_cpuset(&cpu_steal_infos[i]->assigned_mask[j], new_distribution[j], ncpus);
                                debug("new mask %d, size %d",j, CPU_COUNT(&cpu_steal_infos[i]->assigned_mask[j]));
                        }
                        CPU_OR(non_free_mask, non_free_mask, &cpu_steal_infos[i]->assigned_mask[j]);
                        print_cpu_steal_info(cpu_steal_infos[i]);
                        for(c = 0; c < ncpus; c++) {
                                if( CPU_ISSET(c, &to_take_mask) ) {
                                        cpu_steal_infos[i]->stolen[c] = NULL;
                                        cpu_steal_infos[i]->nsteals = cpu_steal_infos[i]->nsteals - 1;
                                }
                                if(!CPU_ISSET(c, non_free_mask))
                                        CPU_SET(c, &free_mask);
                        }
                        xfree(new_distribution);
                }
        }
        return SLURM_SUCCESS;
}
int DLB_Drom_update_masks(int *pids, cpu_set_t *dlb_masks, int npids) {
        int i, j, k, l, m;
        List                  steps;
        ListIterator          ii;
        step_loc_t            *s       = NULL;
        int                   fd;
        uint32_t              *step_pids, nstep_pids;

        steps = stepd_available(conf->spooldir, conf->node_name);
        ii = list_iterator_create(steps);
        while ((s = list_next(ii))) {
                debug("step %d.%d", s->jobid, s->stepid);
                for(i = 0; i < n_active_jobs; i++) {
                        if(s->jobid != cpu_steal_infos[i]->job_id) {
                                debug("not right job, %d %d", i, cpu_steal_infos[i]->job_id);
                                continue;
                        }
                        if (s->stepid == NO_VAL) {
                                debug("skipping batch stepd");
                                break;
                        }

                        debug("updating job %d",s->jobid);
                        fd = stepd_connect(s->directory, s->nodename,
                                           s->jobid, s->stepid,
                                           &s->protocol_version);

                        if (fd == -1) {
                                error("Can't communicate with stepd");
                                break;
                        }

                        stepd_list_pids(fd, s->protocol_version, &step_pids, &nstep_pids);
                        debug("got %d pids", nstep_pids);
                        close(fd);
                        debug("job index in cpu_steal_infos: %d, n active jobs %d", i, n_active_jobs);
                        for(j = 0; j < nstep_pids; j++) {
                                for(k = 0; k < npids; k++) {
                                        if(pids[k] != step_pids[j])
                                                continue;
                                        //TODO: this can be deleted if we associate pids to cpu_steal_info_t structures
                                        int c, maxc=0, match;
                                        for(l = 0; l < cpu_steal_infos[i]->ntasks; l++)
                                                if(CPU_EQUAL(&cpu_steal_infos[i]->assigned_mask[l],&dlb_masks[k]))
                                                        break;
                                        if( l != cpu_steal_infos[i]->ntasks) {//no changes in the mask, go to the next pid
                                                debug("mask of pid %d not changed",pids[k]);
                                                break;
                                        }
                                        for(l = 0; l < cpu_steal_infos[i]->ntasks; l++) {
                                                c = 0;
                                                for(m = 0; m < ncpus; m++)
                                                        if(CPU_ISSET(m, &cpu_steal_infos[i]->assigned_mask[l]) && CPU_ISSET(m, &dlb_masks[k]))
                                                                c++;
                                                if(c > maxc) {
                                                        maxc = c;
                                                        match = l;
                                                }
                                        }
                                        debug("mask of pid %d changed, maxc = %d", pids[k], maxc);
                                        char mask[1 + CPU_SETSIZE / 4];
                                        cpuset_to_str(&cpu_steal_infos[i]->assigned_mask[match], mask);
                                        debug("new mask: %s", mask);
                                        cpuset_to_str(&dlb_masks[k], mask);
                                        debug("old mask: %s", mask);
                                        if(DLB_Drom_SetProcessMask(pids[k],(dlb_cpu_set_t) &cpu_steal_infos[i]->assigned_mask[match])) {
                                                debug("Failed to set DROM process mask of %d",pids[j]);
                                                xfree(dlb_masks);
                                                return SLURM_ERROR;
                                        }
                                }
                        }
                        xfree(step_pids);
                }
        }
        list_iterator_destroy(ii);
        FREE_NULL_LIST(steps);
        return SLURM_SUCCESS;
}

int DLB_Drom_reassign_cpus(uint32_t job_id)
{
        int pids[MAX_PROCS];
        int npids, i, job_index = -1;
        cpu_set_t *dlb_masks = NULL;
        cpu_set_t non_free_mask;
        int changes = 0;
        cpu_steal_info_t *to_destroy;

        debug("In DLB_Drom_reassign_cpus");

        for(i = 0; i < n_active_jobs; i++)
                if(job_id == cpu_steal_infos[i]->job_id) {
                        job_index = i;
                        debug("Index of finised job in cpu_steal_infos: %d", job_index);
                        break;
                }

        to_destroy = cpu_steal_infos[job_index];
        if(job_index < (n_active_jobs-1) && n_active_jobs != 1)
                cpu_steal_infos[job_index] = cpu_steal_infos[n_active_jobs-1];
        n_active_jobs--;
        debug("destroying job %d", to_destroy->job_id);
        n_active_procs -= to_destroy->ntasks;

        get_non_free_mask(&non_free_mask);

        //system("dlb_taskset -l");
        if(to_destroy->nsteals != 0) {
                DLB_Drom_return_to_owner(to_destroy, &non_free_mask);
                debug("cpus returned!");
                changes = 1;
        }
        if(n_active_jobs != 0 && CPU_COUNT(&non_free_mask) < ncpus) {
                if(DLB_Drom_expand_jobs(&non_free_mask) != SLURM_SUCCESS) {
                        error("DLB_Drom_expand_jobs failed");
                        return SLURM_ERROR;
                }
                debug("expanded shrinked jobs");
                changes = 1;
        }
        //set mask with DLB_Drom APIs
        if (changes != 0) {
                if(get_DLB_procs_masks(pids, &dlb_masks, &npids) != SLURM_SUCCESS)
                        return SLURM_ERROR;
                debug("Found %d processes for redistributing resources, n_active_procs = %d", npids, n_active_procs);
                DLB_Drom_update_masks(pids, dlb_masks, npids);
                debug("DLB masks set");
        }
        //system("dlb_taskset -l");

        cpu_steal_info_destroy(to_destroy);

        debug("n_active_jobs: %d", n_active_jobs);
        if(npids != 0)
                xfree(dlb_masks);
        return SLURM_SUCCESS;
}

static int _jobs_equal(void *x, void *key) {
        int *job1 = x, job2 = key;
        if(job1 == job2)
                return 1;
        return 0;
}
int DLB_Drom_wait_for_dependencies(stepd_step_rec_t *job) {
        List                    steps;
        ListIterator            ii;
        step_loc_t              *s = NULL;
        int                     fd, counter = 0,index, steps_count, *ready_vector;
        slurmstepd_state_t      status;

        steps = stepd_available(conf->spooldir, conf->node_name);
        ii = list_iterator_create(steps);
        steps_count = list_count(steps);
        ready_vector = (int *) xmalloc(sizeof(int) * steps_count);

        for(index = 0; index < steps_count; index++)
                ready_vector[index] = 0;

        while (counter != steps_count) {
                index = 0;
                list_iterator_reset(ii);

                while ((s = list_next(ii))) {
                        if(ready_vector[index] == 1) {
                                index++;
                                continue;
                        }
                        //skip batch, job itself,next jobs and not dependent jobs
                        if(s->jobid >= job->jobid || s->stepid == NO_VAL ||
                          (job->job_dependencies && list_find_first(
                           job->job_dependencies, _jobs_equal, &s->jobid) != NULL)) {
                                ready_vector[index] = 1;
                                index++;
                                counter++;
                                continue;
                        }
                        fd = stepd_connect(s->directory, s->nodename,
                                           s->jobid, s->stepid,
                                           &s->protocol_version);

                        if (fd == -1) {
                                error("Can't communicate with stepd");
                                break;
                        }
                        status = stepd_state(fd, s->protocol_version);
                        if(status == SLURMSTEPD_STEP_RUNNING || status == SLURMSTEPD_STEP_ENDING) {
                                debug("step %d.%d is running", s->jobid, s->stepid);
                                counter++;
                                ready_vector[index] = 1;
                        }
                        else
                                debug("step %d.%d is still not running", s->jobid, s->stepid);
                        index++;
                }

                if(counter != steps_count)
                        usleep(DROM_SYNC_WAIT_USECS);
        }

        list_iterator_destroy(ii);
        FREE_NULL_LIST(steps);
        xfree(ready_vector);
        return SLURM_SUCCESS;
}

int DLB_Drom_Register_task(job, cur_mask)
{
	if(!job->batch) {
                struct timeval t1,t2;
                cpu_set_t cur_mask;
                long elapsed;
                gettimeofday(&t1, NULL);

                DLB_Drom_wait_for_dependencies(job);

                slurm_getaffinity(job->envtp->task_pid, sizeof(cur_mask), &cur_mask);
                if(DLB_Drom_PreRegister(job->envtp->task_pid, &cur_mask, 1)) {
                        debug("Error pre registering DROM mask");
                        rc = SLURM_ERROR;
                }

                gettimeofday(&t2, NULL);
                elapsed = (t2.tv_sec-t1.tv_sec) * 1000000 + t2.tv_usec-t1.tv_usec;
                elapsed /= 1000;
                debug("Time taken by pre_launch DROM part: %ld ms", elapsed);
        }
	return SLURM_SUCCESS;
}
