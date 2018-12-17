/*****************************************************************************\
 *  Copyright (C) 2006-2009 Hewlett-Packard Development Company, L.P.
 *  Copyright (C) 2008-2009 Lawrence Livermore National Security.
 *  Written by Susanne M. Balle, <susanne.balle@hp.com>
 *  CODE-OCEC-09-009. All rights reserved.
 *
 *  This file is part of SLURM, a resource management program.
 *  For details, see <http://slurm.schedmd.com/>.
 *  Please also read the included file: DISCLAIMER.
 *
 *  SLURM is free software; you can redistribute it and/or modify it under
 *  the terms of the GNU General Public License as published by the Free
 *  Software Foundation; either version 2 of the License, or (at your option)
 *  any later version.
 *
 *  In addition, as a special exception, the copyright holders give permission
 *  to link the code of portions of this program with the OpenSSL library under
 *  certain conditions as described in each individual source file, and
 *  distribute linked combinations including the two. You must obey the GNU
 *  General Public License in all respects for all of the code used other than
 *  OpenSSL. If you modify file(s) with this exception, you may extend this
 *  exception to your version of the file(s), but you are not obligated to do
 *  so. If you do not wish to do so, delete this exception statement from your
 *  version.  If you delete this exception statement from all source files in
 *  the program, then also delete it here.
 *
 *  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
 *  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 *  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
 *  details.
 *
 *  You should have received a copy of the GNU General Public License along
 *  with SLURM; if not, write to the Free Software Foundation, Inc.,
 *  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
\*****************************************************************************/


#include "affinity.h"
#include "dist_tasks.h"
#include "src/common/bitstring.h"
#include "src/common/log.h"
#include "src/common/slurm_cred.h"
#include "src/common/slurm_protocol_api.h"
#include "src/common/slurm_protocol_defs.h"
#include "src/common/slurm_resource_info.h"
#include "src/common/xmalloc.h"
#include "src/slurmd/slurmd/slurmd.h"

#ifdef HAVE_NUMA
#include <numa.h>
#endif

#ifdef HAVE_HWLOC
#include <hwloc.h>
#endif

#include "dlb_drom.h"
#include "src/common/slurm_extrae.h"
#include <time.h> //for timing
#include <sys/time.h> //for timing
#define DROM_SYNC_WAIT_USECS 100
cpu_steal_info_t *cpu_steal_infos[MAX_PROCS];
uint32_t n_active_jobs = 0;
uint32_t n_active_procs = 0;
int32_t ncpus;
double  DLB_share_factor = 0;

static char *_alloc_mask(launch_tasks_request_msg_t *req,
			 int *whole_node_cnt, int *whole_socket_cnt,
			 int *whole_core_cnt, int *whole_thread_cnt,
			 int *part_socket_cnt, int *part_core_cnt);
static bitstr_t *_get_avail_map(launch_tasks_request_msg_t *req,
				uint16_t *hw_sockets, uint16_t *hw_cores,
				uint16_t *hw_threads);
static int _get_local_node_info(slurm_cred_arg_t *arg, int job_node_id,
				uint16_t *sockets, uint16_t *cores);

static int _task_layout_lllp_block(launch_tasks_request_msg_t *req,
				   uint32_t node_id, bitstr_t ***masks_p);
static int _task_layout_lllp_cyclic(launch_tasks_request_msg_t *req,
				    uint32_t node_id, bitstr_t ***masks_p);

static void _lllp_map_abstract_masks(const uint32_t maxtasks,
				     bitstr_t **masks);
static void _lllp_generate_cpu_bind(launch_tasks_request_msg_t *req,
				    const uint32_t maxtasks,
				    bitstr_t **masks);

/*     BLOCK_MAP     physical machine LLLP index to abstract block LLLP index
 *     BLOCK_MAP_INV physical abstract block LLLP index to machine LLLP index
 */
#define BLOCK_MAP(index)	_block_map(index, conf->block_map)
#define BLOCK_MAP_INV(index)	_block_map(index, conf->block_map_inv)


/* _block_map
 *
 * safely returns a mapped index using a provided block map
 *
 * IN - index to map
 * IN - map to use
 */
static uint16_t _block_map(uint16_t index, uint16_t *map)
{
	if (map == NULL) {
	    	return index;
	}
	/* make sure bit falls in map */
	if (index >= conf->block_map_size) {
		debug3("wrapping index %u into block_map_size of %u",
		       index, conf->block_map_size);
		index = index % conf->block_map_size;
	}
	index = map[index];
	return(index);
}

static void _task_layout_display_masks(launch_tasks_request_msg_t *req,
					const uint32_t *gtid,
					const uint32_t maxtasks,
					bitstr_t **masks)
{
	int i;
	char *str = NULL;
	for(i = 0; i < maxtasks; i++) {
		str = (char *)bit_fmt_hexmask(masks[i]);
		debug3("_task_layout_display_masks jobid [%u:%d] %s",
		       req->job_id, gtid[i], str);
		xfree(str);
	}
}

static void _lllp_free_masks(const uint32_t maxtasks, bitstr_t **masks)
{
    	int i;
	bitstr_t *bitmask;

	for (i = 0; i < maxtasks; i++) {
		bitmask = masks[i];
		FREE_NULL_BITMAP(bitmask);
	}
	xfree(masks);
}

#ifdef HAVE_NUMA
/* _match_mask_to_ldom
 *
 * expand each mask to encompass the whole locality domain
 * within which it currently exists
 * NOTE: this assumes that the masks are already in logical
 * (and not abstract) CPU order.
 */
static void _match_masks_to_ldom(const uint32_t maxtasks, bitstr_t **masks)
{
	uint32_t i, b, size;

	if (!masks || !masks[0])
		return;
	size = bit_size(masks[0]);
	for(i = 0; i < maxtasks; i++) {
		for (b = 0; b < size; b++) {
			if (bit_test(masks[i], b)) {
				/* get the NUMA node for this CPU, and then
				 * set all CPUs in the mask that exist in
				 * the same CPU */
				int c;
				uint16_t nnid = slurm_get_numa_node(b);
				for (c = 0; c < size; c++) {
					if (slurm_get_numa_node(c) == nnid)
						bit_set(masks[i], c);
				}
			}
		}
	}
}
#endif

/*
 * batch_bind - Set the batch request message so as to bind the shell to the
 *	proper resources
 */
void batch_bind(batch_job_launch_msg_t *req)
{
	bitstr_t *req_map, *hw_map;
	slurm_cred_arg_t arg;
	uint16_t sockets=0, cores=0, num_cpus;
	int start, task_cnt=0;

	if (slurm_cred_get_args(req->cred, &arg) != SLURM_SUCCESS) {
		error("task/affinity: job lacks a credential");
		return;
	}
	start = _get_local_node_info(&arg, 0, &sockets, &cores);
	if (start != 0) {
		error("task/affinity: missing node 0 in job credential");
		slurm_cred_free_args(&arg);
		return;
	}
	if ((sockets * cores) == 0) {
		error("task/affinity: socket and core count both zero");
		slurm_cred_free_args(&arg);
		return;
	}

	num_cpus  = MIN((sockets * cores),
			 (conf->sockets * conf->cores));
	req_map = (bitstr_t *) bit_alloc(num_cpus);
	hw_map  = (bitstr_t *) bit_alloc(conf->block_map_size);

#ifdef HAVE_FRONT_END
{
	/* Since the front-end nodes are a shared resource, we limit each job
	 * to one CPU based upon monotonically increasing sequence number */
	static int last_id = 0;
	bit_set(hw_map, ((last_id++) % conf->block_map_size));
	task_cnt = 1;
}
#else
{
	char *str;
	int t, p;

	/* Transfer core_bitmap data to local req_map.
	 * The MOD function handles the case where fewer processes
	 * physically exist than are configured (slurmd is out of
	 * sync with the slurmctld daemon). */
	for (p = 0; p < (sockets * cores); p++) {
		if (bit_test(arg.job_core_bitmap, p))
			bit_set(req_map, (p % num_cpus));
	}

	str = (char *)bit_fmt_hexmask(req_map);
	debug3("task/affinity: job %u core mask from slurmctld: %s",
		req->job_id, str);
	xfree(str);

	for (p = 0; p < num_cpus; p++) {
		if (bit_test(req_map, p) == 0)
			continue;
		/* core_bitmap does not include threads, so we
		 * add them here but limit them to what the job
		 * requested */
		for (t = 0; t < conf->threads; t++) {
			uint16_t pos = p * conf->threads + t;
			if (pos >= conf->block_map_size) {
				info("more resources configured than exist");
				p = num_cpus;
				break;
			}
			bit_set(hw_map, pos);
			task_cnt++;
		}
	}
}
#endif
	if (task_cnt) {
		req->cpu_bind_type = CPU_BIND_MASK;
		if (conf->task_plugin_param & CPU_BIND_VERBOSE)
			req->cpu_bind_type |= CPU_BIND_VERBOSE;
		xfree(req->cpu_bind);
		req->cpu_bind = (char *)bit_fmt_hexmask(hw_map);
		info("task/affinity: job %u CPU input mask for node: %s",
		     req->job_id, req->cpu_bind);
		/* translate abstract masks to actual hardware layout */
		_lllp_map_abstract_masks(1, &hw_map);
#ifdef HAVE_NUMA
		if (req->cpu_bind_type & CPU_BIND_TO_LDOMS) {
			_match_masks_to_ldom(1, &hw_map);
		}
#endif
		xfree(req->cpu_bind);
		req->cpu_bind = (char *)bit_fmt_hexmask(hw_map);
		info("task/affinity: job %u CPU final HW mask for node: %s",
		     req->job_id, req->cpu_bind);
	} else {
		error("task/affinity: job %u allocated no CPUs",
		      req->job_id);
	}
	FREE_NULL_BITMAP(hw_map);
	FREE_NULL_BITMAP(req_map);
	slurm_cred_free_args(&arg);
}

/* The job has specialized cores, synchronize user map with available cores */
static void _validate_map(launch_tasks_request_msg_t *req, char *avail_mask)
{
	char *tmp_map, *save_ptr = NULL, *tok;
	cpu_set_t avail_cpus;
	bool superset = true;

	CPU_ZERO(&avail_cpus);
	(void) str_to_cpuset(&avail_cpus, avail_mask);
	tmp_map = xstrdup(req->cpu_bind);
	tok = strtok_r(tmp_map, ",", &save_ptr);
	while (tok) {
		int i = atoi(tok);
		if (!CPU_ISSET(i, &avail_cpus)) {
			/* The task's CPU map is completely invalid.
			 * Disable CPU map. */
			superset = false;
			break;
		}
		tok = strtok_r(NULL, ",", &save_ptr);
}
	xfree(tmp_map);

	if (!superset) {
		info("task/affinity: Ignoring user CPU binding outside of job "
		     "step allocation");
		req->cpu_bind_type &= (~CPU_BIND_MAP);
		req->cpu_bind_type |=   CPU_BIND_MASK;
		xfree(req->cpu_bind);
		req->cpu_bind = xstrdup(avail_mask);
	}
}

/* The job has specialized cores, synchronize user mask with available cores */
static void _validate_mask(launch_tasks_request_msg_t *req, char *avail_mask)
{
	char *new_mask = NULL, *save_ptr = NULL, *tok;
	cpu_set_t avail_cpus, task_cpus;
	bool superset = true;

	CPU_ZERO(&avail_cpus);
	(void) str_to_cpuset(&avail_cpus, avail_mask);
	tok = strtok_r(req->cpu_bind, ",", &save_ptr);
	while (tok) {
		int i, overlaps = 0;
		char mask_str[1 + CPU_SETSIZE / 4];
		CPU_ZERO(&task_cpus);
		(void) str_to_cpuset(&task_cpus, tok);
		for (i = 0; i < CPU_SETSIZE; i++) {
			if (!CPU_ISSET(i, &task_cpus))
				continue;
			if (CPU_ISSET(i, &avail_cpus)) {
				overlaps++;
			} else {
				CPU_CLR(i, &task_cpus);
				superset = false;
			}
		}
		if (overlaps == 0) {
			/* The task's CPU mask is completely invalid.
			 * Give it all allowed CPUs. */
			for (i = 0; i < CPU_SETSIZE; i++) {
				if (CPU_ISSET(i, &avail_cpus))
					CPU_SET(i, &task_cpus);
			}
		}
		cpuset_to_str(&task_cpus, mask_str);
		if (new_mask)
			xstrcat(new_mask, ",");
		xstrcat(new_mask, mask_str);
		tok = strtok_r(NULL, ",", &save_ptr);
	}

	if (!superset) {
		info("task/affinity: Ignoring user CPU binding outside of job "
		     "step allocation");
	}

	xfree(req->cpu_bind);
	req->cpu_bind = new_mask;
}
void cpu_steal_info_init(cpu_steal_info_t **infos)
{
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

void cpu_steal_info_destroy(cpu_steal_info_t *infos)
{
	debug("destroying infos of %d",infos->job_id);

	xfree(infos->manual_masks);
	xfree(infos->auto_mask);
	xfree(infos->general_mask);
	xfree(infos->assigned_mask);
	xfree(infos->stolen);
	xfree(infos);
}

void print_cpu_steal_info(cpu_steal_info_t *infos)
{
        char str[CPU_SETSIZE+1];
        int i;

        debug("jobid: %d, ntasks: %d, assigned cpt: %d, original cpt: %d," 
	       "nsteals: %d", infos->job_id, infos->ntasks, infos->assigned_cpt,
	       infos->original_cpt, infos->nsteals);

        cpuset_to_str(infos->general_mask, str);
        debug("general mask: %s",str);

        if (infos->manual_masks != NULL)
                for(i = 0; i < infos->ntasks; i++) {
                        cpuset_to_str(&infos->manual_masks[i], str);
                        debug("manual mask: %s",str);
                }
        if (infos->assigned_mask)
                for(i = 0; i < infos->ntasks; i++) {
                        cpuset_to_str(&infos->assigned_mask[i], str);
                        debug("assigned mask: %s",str);
                }
        if(infos->auto_mask) {
                cpuset_to_str(infos->auto_mask, str);
        	debug("auto mask: %s",str);
        }
	for(i = 0; i < ncpus; i++)
                if(infos->stolen[i])
                        debug("cpu %d stolen",i);
}

int get_DLB_procs_masks(int *dlb_pids, cpu_set_t **dlb_masks, int *npids)
{
        int j, error;
        cpu_set_t *masks;

	debug("In get_DLB_procs_masks");
	DLB_DROM_Attach();
	DLB_DROM_GetPidList(dlb_pids, npids, MAX_PROCS);
        debug("DLB found %d processes", *npids);
        if(*npids == 0) {
		masks = NULL;
		DLB_DROM_Detach();
                return SLURM_SUCCESS;
	}

        masks = (cpu_set_t *) xmalloc(sizeof(cpu_set_t)*(*npids));

        for(j = 0; j < *npids; j++)
                if((error = 
		    DLB_DROM_GetProcessMask(dlb_pids[j], &masks[j], 0))) {
                        debug("Error in DLB_Drom_GetProcessMask for pid %d, "
			      "error %d", dlb_pids[j], error);
			xfree(masks);
                    	DLB_DROM_Detach();
			return SLURM_ERROR;
                }
	DLB_DROM_Detach();
        *dlb_masks = masks;
        return SLURM_SUCCESS;
}

void get_conflict_masks(cpu_set_t *req_set, int nsets, cpu_set_t *conflict_mask)
{
        int i, j, k;
        cpu_set_t mask;

        debug("In get_conflict_masks");

	CPU_ZERO(conflict_mask);
        for(i = 0; i < nsets; i++)
                for(j = 0; j < n_active_jobs; j++)
			for(k = 0; k < cpu_steal_infos[j]->ntasks; k++) {
				//get CPUs in common between active DLB
				//processes and entering Job
                        	CPU_AND(&mask, &req_set[i],
					&cpu_steal_infos[j]->assigned_mask[k]);
				//add them to conflict mask
                        	CPU_OR(conflict_mask, &mask, conflict_mask);
			}
}

int steal_cpus_from_proc(int index, int proc_index, cpu_steal_info_t *steal_infos,
			 cpu_set_t *steal_mask, cpu_set_t *final_mask,
			 cpu_set_t *conflict_mask, int to_steal)
{
        int i, k, stolen = 0;
	cpu_steal_info_t *victim = cpu_steal_infos[index];
	char str[CPU_SETSIZE+1];

	debug("In steal_cpus_from_proc");

	//try to give consecutive cpus 
	//TODO:implement some isolated first policy? give isolated cpus first
	//or steal from less occupied socket first (if to_steal is low enough)
	//or steal one socket first and redistribute
        for(i = ncpus - 1; i >= 0; i--) {
		cpuset_to_str(steal_mask, str);
		debug3("steal_mask: %s", str);
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
//TODO: a paritá di value, ordina per numero di cpus consecutive
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
	List job_dependencies = list_create(slurm_free_job_dependency);
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
		float *procs = xmalloc(sizeof(float) * cpu_steal_infos[j]->ntasks);
		int *ordered_procs = xmalloc(sizeof(int) * cpu_steal_infos[j]->ntasks);
		
		for(i = 0; i < cpu_steal_infos[j]->ntasks; i++) {
			procs[i] = (float) CPU_COUNT(&cpu_steal_infos[j]->assigned_mask[i]);
			ordered_procs[i] = i;
		}
		sort_by_values(ordered_procs, procs, cpu_steal_infos[j]->ntasks);
		max_steal_per_task = max_steal[j] / cpu_steal_infos[j]->ntasks;
		for(i = 0; i < cpu_steal_infos[j]->ntasks && tot_stolen < final_steal; i++) {
			//TODO:we can steal first on one socket, than on second, to group steal, we simply need to filter conflict mask
			//TODO:bring this out, we are at task level here, not job level, 
			//     max_steal got stolen and nsteals are at job level!!!
			int index = ordered_procs[cpu_steal_infos[j]->ntasks - 1 - i];
			CPU_AND(&steal_mask, &cpu_steal_infos[j]->assigned_mask[index], conflict_mask);
                	cpuset_to_str(&steal_mask, str);
                	debug3("steal_mask: %s", str);	
			cpuset_to_str(&cpu_steal_infos[j]->assigned_mask[index], str);
                        debug3("assigned_mask: %s", str);
			cpuset_to_str(conflict_mask, str);
                        debug3("conflict_mask: %s", str);
			to_steal = values[j] > 1 ? values[j] : 1;
			//TODO: try: steal one per time instead of getting it from values vector
			to_steal = 1;
			if(to_steal > max_steal_per_task)
				to_steal = max_steal_per_task;
			//TODO:bring this out, we are at task level here, not job level, 
                        //     max_steal got stolen and nsteals are at job level!!!
			if(to_steal + cpu_steal_infos[j]->got_stolen + cpu_steal_infos[j]->nsteals > max_steal[j])
				to_steal = max_steal[j] - cpu_steal_infos[j]->got_stolen - cpu_steal_infos[j]->nsteals;
			if(to_steal == CPU_COUNT(&cpu_steal_infos[j]->assigned_mask[index])) {
                        	debug("Trying to steal all cpus from proc %d.%d", cpu_steal_infos[j]->job_id, index);
                        	to_steal--;
                	}

			if((to_steal + tot_stolen) > final_steal)
                        	to_steal = final_steal - tot_stolen;
		
			debug("requested %d cpus already in use, stealing %d from step %d.%d, value: %f", final_steal, to_steal, cpu_steal_infos[j]->job_id, index, values[j]);
                	stolen = steal_cpus_from_proc(j, index, steal_infos, &steal_mask, final_mask, conflict_mask, to_steal);
			if(stolen > 0) {
				tot_stolen += stolen;
				cpu_steal_infos[j]->got_stolen += stolen;
				values[j] -= stolen;
				uint32_t *item = xmalloc(sizeof(uint32_t));
				*item = cpu_steal_infos[j]->job_id;
				list_append(job_dependencies, item);
                	}
               	 	else {
                        	values[j] = -1;
                        	nordered--;
                	}
                	sort_by_values(ordered,values,nordered);
		}
		xfree(ordered_procs);
		xfree(procs);
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

/*void wait_for_dependent_processes() {
	int dlb_pids[MAX_PROCS], npids;
	        DLB_Drom_GetPidList(dlb_pids, &npids, MAX_PROCS);
	while(n_active_procs > npids) {
		debub("waiting %d processes to start, %d alive", n_active_procs, npids);
		usleep(DROM_SYNC_WAIT_USECS);	
		DLB_Drom_GetPidList(dlb_pids, &npids, MAX_PROCS);
	}
}*/

int DLB_Drom_steal_cpus(launch_tasks_request_msg_t *req, uint32_t node_id) {
        int tot_steal, final_steal, cpus_per_task = 0;
	cpu_set_t conflict_mask, *final_mask = NULL, non_free_mask;
        char *new_mask = NULL, mask_str[1 + CPU_SETSIZE / 4];
	int i, nsets;
	cpu_steal_info_t *steal_infos;
//	partition_info_msg_t *part_ptr = NULL;
	slurm_ctl_conf_t *config;	
	
	debug("In DLB_Drom_steal_cpus");
	debug("n active jobs: %d",n_active_jobs);
	//TODO:make SF dynamic?
	config = slurm_conf_lock();
	DLB_share_factor = config->sharing_factor;
	slurm_conf_unlock();
	//debug("share factor: %lf", DLB_share_factor);
//	slurm_free_partition_info_msg(part_ptr);
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
		
		ListIterator ii = list_iterator_create(job_dependencies);
        	uint32_t *jobid;
		while ((jobid = list_next(ii))) {
			debug("job %d is a dependency", *jobid);
		}	
		list_iterator_destroy(ii);
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
//	if(steal_infos->nsteals != 0)
//                wait_for_dependent_processes();

	//store in cpu_steal_info vector
	cpu_steal_infos[n_active_jobs] = steal_infos;
	print_cpu_steal_info(cpu_steal_infos[n_active_jobs]);
	n_active_jobs++;
	n_active_procs += steal_infos->ntasks; 
	debug("n_active_jobs: %d", n_active_jobs);
	if(req->cpu_bind)
		xfree(req->cpu_bind);
	req->cpu_bind = new_mask;
       
	return SLURM_SUCCESS;
}

int DLB_Drom_return_to_owner(cpu_steal_info_t *to_destroy, cpu_set_t * nonfreemask) {
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

/* helper function for _expand_masks() */
static void _blot_mask(bitstr_t *mask, bitstr_t *avail_map, uint16_t blot)
{
        uint16_t i, j, size = 0;
        int prev = -1;

        if (!mask)
                return;
        size = bit_size(mask);
        for (i = 0; i < size; i++) {
                if (bit_test(mask, i)) {
                        /* fill in this blot */
                        uint16_t start = (i / blot) * blot;
                        if (start != prev) {
                                for (j = start; j < start + blot; j++) {
                                        if (bit_test(avail_map, j))
                                                bit_set(mask, j);
                                }
                                prev = start;
                        }
                }
        }
}

/* helper function for _expand_masks()
 * for each task, consider which other bits are set in avail_map
 * on the same socket */
static void _blot_mask_sockets(const uint32_t maxtasks, const uint32_t task,
                               bitstr_t **masks, uint16_t hw_sockets,
                               uint16_t hw_cores, uint16_t hw_threads,
                               bitstr_t *avail_map)
{
        uint16_t i, j, size = 0;
        int blot;

        if (!masks[task])
                return;

        blot = bit_size(avail_map) / hw_sockets;
        if (blot <= 0)
                blot = 1;
        size = bit_size(masks[task]);
        for (i = 0; i < size; i++) {
                if (bit_test(masks[task], i)) {
                        /* check if other bits are set in avail_map on this
                         * socket and set each corresponding bit in masks */
                        uint16_t start = (i / blot) * blot;
                        for (j = start; j < start+blot; j++) {
                                if (bit_test(avail_map, j))
                                        bit_set(masks[task], j);
                        }
                }
        }
}


/* for each mask, expand the mask around the set bits to include the
 * complete resource to which the set bits are to be bound */
static void _expand_masks(uint16_t cpu_bind_type, const uint32_t maxtasks,
                          bitstr_t **masks, uint16_t hw_sockets,
                          uint16_t hw_cores, uint16_t hw_threads,
                          bitstr_t *avail_map)
{
        uint32_t i;

        if (cpu_bind_type & CPU_BIND_TO_THREADS)
                return;
        if (cpu_bind_type & CPU_BIND_TO_CORES) {
                if (hw_threads < 2)
                        return;
                for (i = 0; i < maxtasks; i++) {
                        _blot_mask(masks[i], avail_map, hw_threads);
                }
                return;
        }
        if (cpu_bind_type & CPU_BIND_TO_SOCKETS) {
                if (hw_threads*hw_cores < 2)
                        return;
                for (i = 0; i < maxtasks; i++) {
                        _blot_mask_sockets(maxtasks, i, masks, hw_sockets,
                                           hw_cores, hw_threads, avail_map);
                }
                return;
        }
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
	//	debug("mask %d: %s",s,bit_fmt_hexmask(bit_to_masks[s]));
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
			for(j = 0; j < cpu_steal_infos[i]->ntasks; j++)
				xfree(new_distribution[j]);
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
					int error;
					DLB_DROM_Attach();
					if((error = DLB_DROM_SetProcessMask(pids[k],(dlb_cpu_set_t) &cpu_steal_infos[i]->assigned_mask[match], 0))) {
                               			debug("Failed to set DROM process mask of %d, error: %d",pids[j], error);
                				DLB_DROM_Detach();       
		        			return SLURM_ERROR;
                        		}
					DLB_DROM_Detach();
					//update extrae infos
                                        //first stop moved threads
                                        for(m = 0; m < ncpus; m++)
                                                if(CPU_ISSET(m, &dlb_masks[k]) && !CPU_ISSET(m, &cpu_steal_infos[i]->assigned_mask[match]))
                                                        slurmd_extrae_stop_thread(m);
                                        //then start new ones
                                        for(m = 0; m < ncpus; m++)
                                                if(!CPU_ISSET(m, &dlb_masks[k]) && CPU_ISSET(m, &cpu_steal_infos[i]->assigned_mask[match])) {
                                                        int thread_id = slurmd_get_next_extrae_thread(cpu_steal_infos[i]->job_id, match + 1 + cpu_steal_infos[i]->first_gtid);
                                                        if(thread_id == -1) {
                                                                debug("Error with extrae thread ids");
                                                                break;
                                                        }
                                                        slurmd_extrae_start_thread(cpu_steal_infos[i]->job_id, m, match + 1 + cpu_steal_infos[i]->first_gtid, thread_id);
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
        int npids, i, cpu_id, job_index = -1;
        cpu_set_t *dlb_masks = NULL;
        cpu_set_t non_free_mask;
	int changes = 0;
	cpu_steal_info_t *to_destroy = NULL;
        
        debug("In DLB_Drom_reassign_cpus");

	for(i = 0; i < n_active_jobs; i++)
                if(job_id == cpu_steal_infos[i]->job_id) {
                        job_index = i;
                        debug("Index of finised job in cpu_steal_infos: %d", job_index);
                        break;
                }
	if(i == n_active_jobs) {
		debug("No info about this job, some error occurred");
		return SLURM_ERROR;
	}
        to_destroy = cpu_steal_infos[job_index];

	debug("updating extrae trace");
        for(i = 0; i < to_destroy->ntasks; i++)
                for(cpu_id = 0; cpu_id < ncpus; cpu_id++)
                        if(CPU_ISSET(cpu_id, &to_destroy->assigned_mask[i])) {
                                slurmd_extrae_stop_thread(cpu_id);
                        }

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

	debug("updating extrae trace");
        for(i = 0; i < to_destroy->ntasks; i++)
		for(cpu_id = 0; cpu_id < ncpus; cpu_id++)
                	if(CPU_ISSET(cpu_id, &to_destroy->assigned_mask[i])) {
				slurmd_extrae_stop_thread(cpu_id); 
			}

	//set mask with DLB_Drom APIs
	if (changes != 0) {
		if(get_DLB_procs_masks(pids, &dlb_masks, &npids) != SLURM_SUCCESS)
                	return SLURM_ERROR;
        	debug("Found %d processes for redistributing resources, n_active_procs = %d", npids, n_active_procs);
		DLB_Drom_update_masks(pids, dlb_masks, npids);	
		debug("DLB masks set");
	}
	cpu_steal_info_destroy(to_destroy);

	debug("n_active_jobs: %d", n_active_jobs);
	if(npids != 0)
		xfree(dlb_masks);
        return SLURM_SUCCESS;
}

/*
 * lllp_distribution
 *
 * Note: lllp stands for Lowest Level of Logical Processors.
 *
 * When automatic binding is enabled:
 *      - no binding flags set >= CPU_BIND_NONE, and
 *      - a auto binding level selected CPU_BIND_TO_{SOCKETS,CORES,THREADS}
 * Otherwise limit job step to the allocated CPUs
 *
 * generate the appropriate cpu_bind type and string which results in
 * the specified lllp distribution.
 *
 * IN/OUT- job launch request (cpu_bind_type and cpu_bind updated)
 * IN- global task id array
 */
void lllp_distribution(launch_tasks_request_msg_t *req, uint32_t node_id)
{
	int rc = SLURM_SUCCESS;
	bitstr_t **masks = NULL;
	char buf_type[100];
	int maxtasks = req->tasks_to_launch[(int)node_id];
	int whole_nodes, whole_sockets, whole_cores, whole_threads;
	int part_sockets, part_cores;
	const uint32_t *gtid = req->global_task_ids[(int)node_id];
	static uint16_t bind_entity = CPU_BIND_TO_THREADS | CPU_BIND_TO_CORES |
				      CPU_BIND_TO_SOCKETS | CPU_BIND_TO_LDOMS;
	static uint16_t bind_mode = CPU_BIND_NONE   | CPU_BIND_MASK   |
				    CPU_BIND_RANK   | CPU_BIND_MAP    |
				    CPU_BIND_LDMASK | CPU_BIND_LDRANK |
				    CPU_BIND_LDMAP;
	static int only_one_thread_per_core = -1;

	if (only_one_thread_per_core == -1) {
		if (conf->cpus == (conf->sockets * conf->cores))
			only_one_thread_per_core = 1;
		else
			only_one_thread_per_core = 0;
	}

	/* If we are telling the system we only want to use 1 thread
	 * per core with the CPUs node option this is the easiest way
	 * to portray that to the affinity plugin.
	 */
	if (only_one_thread_per_core)
		req->cpu_bind_type |= CPU_BIND_ONE_THREAD_PER_CORE;

	if (req->cpu_bind_type & bind_mode) {
		/* Explicit step binding specified by user */
		char *avail_mask = _alloc_mask(req,
					       &whole_nodes,  &whole_sockets,
					       &whole_cores,  &whole_threads,
					       &part_sockets, &part_cores);

			

		if ((whole_nodes == 0) && avail_mask &&
		    (req->job_core_spec == (uint16_t) NO_VAL)) {
			info("task/affinity: entire node must be allocated, "
			     "disabling affinity");
			xfree(req->cpu_bind);
			req->cpu_bind = avail_mask;
			req->cpu_bind_type &= (~bind_mode);
			req->cpu_bind_type |= CPU_BIND_MASK;
		} else {
			if (req->job_core_spec == (uint16_t) NO_VAL) {
				if (req->cpu_bind_type & CPU_BIND_MASK)
					_validate_mask(req, avail_mask);
				else if (req->cpu_bind_type & CPU_BIND_MAP)
					_validate_map(req, avail_mask);
			}
			xfree(avail_mask);
		}
		//Marco D'Amico: call to stealing function, manual mask case
		if(DLB_Drom_steal_cpus(req, node_id) != SLURM_SUCCESS)
			debug("error in DLB_Drom_steal_cpus");
		slurm_sprint_cpu_bind_type(buf_type, req->cpu_bind_type);
		info("lllp_distribution jobid [%u] manual binding: %s",
		     req->job_id, buf_type);
		return;
	}
//	(void) tr_to_cpuset(&req_set, avail_mask);

	if (!(req->cpu_bind_type & bind_entity)) {
		/* No bind unit (sockets, cores) specified by user,
		 * pick something reasonable */
		uint32_t task_plugin_param = slurm_get_task_plugin_param();
		bool auto_def_set = false;
		int spec_thread_cnt = 0;
		int max_tasks = req->tasks_to_launch[(int)node_id] *
			req->cpus_per_task;
		char *avail_mask = _alloc_mask(req,
					       &whole_nodes,  &whole_sockets,
					       &whole_cores,  &whole_threads,
					       &part_sockets, &part_cores);
		debug("binding tasks:%d to "
		      "nodes:%d sockets:%d:%d cores:%d:%d threads:%d",
		      max_tasks, whole_nodes, whole_sockets ,part_sockets,
		      whole_cores, part_cores, whole_threads);
		if ((req->job_core_spec != (uint16_t) NO_VAL) &&
		    (req->job_core_spec &  CORE_SPEC_THREAD)  &&
		    (req->job_core_spec != CORE_SPEC_THREAD)) {
			spec_thread_cnt = req->job_core_spec &
					  (~CORE_SPEC_THREAD);
		}
		if (((max_tasks == whole_sockets) && (part_sockets == 0)) ||
		    (spec_thread_cnt &&
		     (max_tasks == (whole_sockets + part_sockets)))) {
			req->cpu_bind_type |= CPU_BIND_TO_SOCKETS;
			goto make_auto;
		}
		if (((max_tasks == whole_cores) && (part_cores == 0)) ||
		    (spec_thread_cnt &&
		     (max_tasks == (whole_cores + part_cores)))) {
			req->cpu_bind_type |= CPU_BIND_TO_CORES;
			goto make_auto;
		}
		if (max_tasks == whole_threads) {
			req->cpu_bind_type |= CPU_BIND_TO_THREADS;
			goto make_auto;
		}

		if (task_plugin_param & CPU_AUTO_BIND_TO_THREADS) {
			auto_def_set = true;
			req->cpu_bind_type |= CPU_BIND_TO_THREADS;
			goto make_auto;
		} else if (task_plugin_param & CPU_AUTO_BIND_TO_CORES) {
			auto_def_set = true;
			req->cpu_bind_type |= CPU_BIND_TO_CORES;
			goto make_auto;
		} else if (task_plugin_param & CPU_AUTO_BIND_TO_SOCKETS) {
			auto_def_set = true;
			req->cpu_bind_type |= CPU_BIND_TO_SOCKETS;
			goto make_auto;
		}

		if (avail_mask) {
			xfree(req->cpu_bind);
			req->cpu_bind = avail_mask;
			req->cpu_bind_type |= CPU_BIND_MASK;
		}

		slurm_sprint_cpu_bind_type(buf_type, req->cpu_bind_type);
		info("lllp_distribution jobid [%u] auto binding off: %s",
		     req->job_id, buf_type);
		return;

  make_auto:	xfree(avail_mask);
		slurm_sprint_cpu_bind_type(buf_type, req->cpu_bind_type);
		info("lllp_distribution jobid [%u] %s auto binding: "
		     "%s, dist %d", req->job_id,
		     (auto_def_set) ? "default" : "implicit",
		     buf_type, req->task_dist);
	} else {
		/* Explicit bind unit (sockets, cores) specified by user */
		slurm_sprint_cpu_bind_type(buf_type, req->cpu_bind_type);
		info("lllp_distribution jobid [%u] binding: %s, dist %d",
		     req->job_id, buf_type, req->task_dist);
	}

        //Marco D'Amico: call to stealing function, auto distribution case
  	struct timeval t1,t2;
	gettimeofday(&t1, NULL); 
	if(DLB_Drom_steal_cpus(req, node_id) != SLURM_SUCCESS) {
               	debug("error in DLB_Drom_steal_cpus");
		rc = SLURM_ERROR;
	}
	gettimeofday(&t2, NULL);
	long elapsed = (t2.tv_sec-t1.tv_sec) * 1000000 + t2.tv_usec-t1.tv_usec;
	elapsed /= 1000;
	debug("Time taken by DLB_Drom_steal_cpus: %ld ms", elapsed);
		
	switch (req->task_dist & SLURM_DIST_STATE_BASE) {
	case SLURM_DIST_BLOCK_BLOCK:
	case SLURM_DIST_CYCLIC_BLOCK:
	case SLURM_DIST_PLANE:
		/* tasks are distributed in blocks within a plane */
		rc = _task_layout_lllp_block(req, node_id, &masks);
		break;
	case SLURM_DIST_ARBITRARY:
	case SLURM_DIST_BLOCK:
	case SLURM_DIST_CYCLIC:
	case SLURM_DIST_UNKNOWN:
		if (slurm_get_select_type_param()
		    & CR_CORE_DEFAULT_DIST_BLOCK) {
			rc = _task_layout_lllp_block(req, node_id, &masks);
			break;
		}
		/* We want to fall through here if we aren't doing a
		   default dist block.
		*/
	default:
		rc = _task_layout_lllp_cyclic(req, node_id, &masks);
		break;
	}
	//update steal_infos with final distribution
	debug("updating cpu_steal structure with final assigned masks");
	if(cpu_steal_infos[n_active_jobs-1]->assigned_mask)
		debug("memory leak???");

	cpu_steal_infos[n_active_jobs-1]->assigned_mask = (cpu_set_t *) xmalloc(sizeof(cpu_set_t) * cpu_steal_infos[n_active_jobs-1]->ntasks);
	int i;	
	for(i = 0; i < cpu_steal_infos[n_active_jobs-1]->ntasks; i++) {
		CPU_ZERO(&cpu_steal_infos[n_active_jobs-1]->assigned_mask[i]);
		bitstr_to_cpuset(&cpu_steal_infos[n_active_jobs-1]->assigned_mask[i], masks[i], ncpus);
	}
	debug("Assigned mask, auto generated case:");
	//print_cpu_steal_info(cpu_steal_infos[n_active_jobs-1]);
	
	debug("extrae info generation");
	int cpu_id, cpu_count;

	cpu_steal_infos[n_active_jobs-1]->first_gtid = gtid[0];

	for(i = 0; i < req->tasks_to_launch[node_id]; i++) {
		cpu_count = 0;
		for(cpu_id = 0; cpu_id < ncpus; cpu_id++) {
			if(cpu_count > req->cpus_per_task)
				break;
			if(CPU_ISSET(cpu_id, &cpu_steal_infos[n_active_jobs-1]->assigned_mask[i])) {
				cpu_count++;
				slurmd_extrae_start_thread(req->job_id, cpu_id, i + 1 + cpu_steal_infos[n_active_jobs-1]->first_gtid, cpu_count);
			}
		}
	}
//	char *tok, *save_ptr;
//	int i = 0;
//	tok = strtok_r(req->cpu_bind, ",", &save_ptr);
//        while(tok) {
//		CPU_ZERO(&cpu_steal_infos[n_active_jobs-1]->assigned_mask[i]);
//                (void) str_to_cpuset(&cpu_steal_infos[n_active_jobs-1]->assigned_mask[i], tok);
//                tok = strtok_r(NULL, ",", &save_ptr);
//                i++;
//        }
		
	/* FIXME: I'm worried about core_bitmap with CPU_BIND_TO_SOCKETS &
	 * max_cores - does select/cons_res plugin allocate whole
	 * socket??? Maybe not. Check srun man page.
	 */

	if (rc == SLURM_SUCCESS) {
		_task_layout_display_masks(req, gtid, maxtasks, masks);
	    	/* translate abstract masks to actual hardware layout */
		_lllp_map_abstract_masks(maxtasks, masks);
		_task_layout_display_masks(req, gtid, maxtasks, masks);
#ifdef HAVE_NUMA
		if (req->cpu_bind_type & CPU_BIND_TO_LDOMS) {
			_match_masks_to_ldom(maxtasks, masks);
			_task_layout_display_masks(req, gtid, maxtasks, masks);
		}
#endif
	    	 /* convert masks into cpu_bind mask string */
		 _lllp_generate_cpu_bind(req, maxtasks, masks);
	} else {
		char *avail_mask = _alloc_mask(req,
					       &whole_nodes,  &whole_sockets,
					       &whole_cores,  &whole_threads,
					       &part_sockets, &part_cores);
		if (avail_mask) {
			xfree(req->cpu_bind);
			req->cpu_bind = avail_mask;
			req->cpu_bind_type &= (~bind_mode);
			req->cpu_bind_type |= CPU_BIND_MASK;
		}
		slurm_sprint_cpu_bind_type(buf_type, req->cpu_bind_type);
		error("lllp_distribution jobid [%u] overriding binding: %s",
		      req->job_id, buf_type);
		error("Verify socket/core/thread counts in configuration");
	}
	if (masks)
		_lllp_free_masks(maxtasks, masks);
}

/*
 * _get_local_node_info - get job allocation details for this node
 * IN: req         - launch request structure
 * IN: job_node_id - index of the local node in the job allocation
 * IN/OUT: sockets - pointer to socket count variable
 * IN/OUT: cores   - pointer to cores_per_socket count variable
 * OUT:  returns the core_bitmap index of the first core for this node
 */
static int _get_local_node_info(slurm_cred_arg_t *arg, int job_node_id,
				uint16_t *sockets, uint16_t *cores)
{
	int bit_start = 0, bit_finish = 0;
	int i, index = -1, cur_node_id = -1;

	do {
		index++;
		for (i = 0; i < arg->sock_core_rep_count[index] &&
			     cur_node_id < job_node_id; i++) {
			bit_start = bit_finish;
			bit_finish += arg->sockets_per_node[index] *
					arg->cores_per_socket[index];
			cur_node_id++;
		}

	} while (cur_node_id < job_node_id);

	*sockets = arg->sockets_per_node[index];
	*cores   = arg->cores_per_socket[index];
	return bit_start;
}

/* Determine which CPUs a job step can use.
 * OUT whole_<entity>_count - returns count of whole <entities> in this
 *                            allocation for this node
 * OUT part__<entity>_count - returns count of partial <entities> in this
 *                            allocation for this node
 * RET - a string representation of the available mask or NULL on error
 * NOTE: Caller must xfree() the return value. */
static char *_alloc_mask(launch_tasks_request_msg_t *req,
			 int *whole_node_cnt,  int *whole_socket_cnt,
			 int *whole_core_cnt,  int *whole_thread_cnt,
			 int *part_socket_cnt, int *part_core_cnt)
{
	uint16_t sockets, cores, threads;
	int c, s, t, i;
	int c_miss, s_miss, t_miss, c_hit, t_hit;
	bitstr_t *alloc_bitmap;
	char *str_mask;
	bitstr_t *alloc_mask;

	*whole_node_cnt   = 0;
	*whole_socket_cnt = 0;
	*whole_core_cnt   = 0;
	*whole_thread_cnt = 0;
	*part_socket_cnt  = 0;
	*part_core_cnt    = 0;

	alloc_bitmap = _get_avail_map(req, &sockets, &cores, &threads);
	if (!alloc_bitmap)
		return NULL;

	alloc_mask = bit_alloc(bit_size(alloc_bitmap));

	i = 0;
	for (s=0, s_miss=false; s<sockets; s++) {
		for (c=0, c_hit=c_miss=false; c<cores; c++) {
			for (t=0, t_hit=t_miss=false; t<threads; t++) {
				/* If we are pretending we have a
				   larger system than we really have
				   this is needed to make sure we
				   don't bust the bank.
				*/
				if (i >= bit_size(alloc_bitmap))
					i = 0;
				if (bit_test(alloc_bitmap, i)) {
					bit_set(alloc_mask, i);
					(*whole_thread_cnt)++;
					t_hit = true;
					c_hit = true;
				} else
					t_miss = true;
				i++;
			}
			if (!t_miss)
				(*whole_core_cnt)++;
			else {
				if (t_hit)
					(*part_core_cnt)++;
				c_miss = true;
			}
		}
		if (!c_miss)
			(*whole_socket_cnt)++;
		else {
			if (c_hit)
				(*part_socket_cnt)++;
			s_miss = true;
		}
	}
	if (!s_miss)
		(*whole_node_cnt)++;
	FREE_NULL_BITMAP(alloc_bitmap);

	if ((req->job_core_spec != (uint16_t) NO_VAL) &&
	    (req->job_core_spec &  CORE_SPEC_THREAD)  &&
	    (req->job_core_spec != CORE_SPEC_THREAD)) {
		int spec_thread_cnt;
		spec_thread_cnt = req->job_core_spec & (~CORE_SPEC_THREAD);
		for (t = threads - 1;
		     ((t > 0) && (spec_thread_cnt > 0)); t--) {
			for (c = cores - 1;
			     ((c > 0) && (spec_thread_cnt > 0)); c--) {
				for (s = sockets - 1;
				     ((s >= 0) && (spec_thread_cnt > 0)); s--) {
					i = s * cores + c;
					i = (i * threads) + t;
					bit_clear(alloc_mask, i);
					spec_thread_cnt--;
				}
			}
		}
	}

	/* translate abstract masks to actual hardware layout */
	_lllp_map_abstract_masks(1, &alloc_mask);

#ifdef HAVE_NUMA
	if (req->cpu_bind_type & CPU_BIND_TO_LDOMS) {
		_match_masks_to_ldom(1, &alloc_mask);
	}
#endif

	str_mask = bit_fmt_hexmask(alloc_mask);
	FREE_NULL_BITMAP(alloc_mask);
	return str_mask;
}

/*
 * Given a job step request, return an equivalent local bitmap for this node
 * IN req          - The job step launch request
 * OUT hw_sockets  - number of actual sockets on this node
 * OUT hw_cores    - number of actual cores per socket on this node
 * OUT hw_threads  - number of actual threads per core on this node
 * RET: bitmap of processors available to this job step on this node
 *      OR NULL on error
 */
static bitstr_t *_get_avail_map(launch_tasks_request_msg_t *req,
				uint16_t *hw_sockets, uint16_t *hw_cores,
				uint16_t *hw_threads)
{
	bitstr_t *req_map, *hw_map;
	slurm_cred_arg_t arg;
	uint16_t p, t, new_p, num_cpus, sockets, cores;
	int job_node_id;
	int start;
	char *str;
	int spec_thread_cnt = 0;

	*hw_sockets = conf->sockets;
	*hw_cores   = conf->cores;
	*hw_threads = conf->threads;

	if (slurm_cred_get_args(req->cred, &arg) != SLURM_SUCCESS) {
		error("task/affinity: job lacks a credential");
		return NULL;
	}

	/* we need this node's ID in relation to the whole
	 * job allocation, not just this jobstep */
	job_node_id = nodelist_find(arg.job_hostlist, conf->node_name);
	start = _get_local_node_info(&arg, job_node_id, &sockets, &cores);
	if (start < 0) {
		error("task/affinity: missing node %d in job credential",
		      job_node_id);
		slurm_cred_free_args(&arg);
		return NULL;
	}
	debug3("task/affinity: slurmctld s %u c %u; hw s %u c %u t %u",
	       sockets, cores, *hw_sockets, *hw_cores, *hw_threads);

	num_cpus = MIN((sockets * cores), ((*hw_sockets)*(*hw_cores)));
	req_map = (bitstr_t *) bit_alloc(num_cpus);
	hw_map  = (bitstr_t *) bit_alloc(conf->block_map_size);

	/* Transfer core_bitmap data to local req_map.
	 * The MOD function handles the case where fewer processes
	 * physically exist than are configured (slurmd is out of
	 * sync with the slurmctld daemon). */
	for (p = 0; p < (sockets * cores); p++) {
		if (bit_test(arg.step_core_bitmap, start+p))
			bit_set(req_map, (p % num_cpus));
	}

	str = (char *)bit_fmt_hexmask(req_map);
	debug3("task/affinity: job %u.%u core mask from slurmctld: %s",
		req->job_id, req->job_step_id, str);
	xfree(str);

	for (p = 0; p < num_cpus; p++) {
		if (bit_test(req_map, p) == 0)
			continue;
		/* If we are pretending we have a larger system than
		   we really have this is needed to make sure we
		   don't bust the bank.
		*/
		new_p = p % conf->block_map_size;
		/* core_bitmap does not include threads, so we
		 * add them here but limit them to what the job
		 * requested */
		for (t = 0; t < (*hw_threads); t++) {
			uint16_t bit = new_p * (*hw_threads) + t;
			bit %= conf->block_map_size;
			bit_set(hw_map, bit);
		}
	}

	if ((req->job_core_spec != (uint16_t) NO_VAL) &&
	    (req->job_core_spec &  CORE_SPEC_THREAD)  &&
	    (req->job_core_spec != CORE_SPEC_THREAD)) {
		spec_thread_cnt = req->job_core_spec & (~CORE_SPEC_THREAD);
	}
	if (spec_thread_cnt) {
		/* Skip specialized threads as needed */
		int i, t, c, s;
		for (t = conf->threads - 1;
		     ((t >= 0) && (spec_thread_cnt > 0)); t--) {
			for (c = conf->cores - 1;
			     ((c >= 0) && (spec_thread_cnt > 0)); c--) {
				for (s = conf->sockets - 1;
				     ((s >= 0) && (spec_thread_cnt > 0)); s--) {
					i = s * conf->cores + c;
					i = (i * conf->threads) + t;
					bit_clear(hw_map, i);
					spec_thread_cnt--;
				}
			}
		}
	}

	str = (char *)bit_fmt_hexmask(hw_map);
	debug3("task/affinity: job %u.%u CPU final mask for local node: %s",
		req->job_id, req->job_step_id, str);
	xfree(str);

	FREE_NULL_BITMAP(req_map);
	slurm_cred_free_args(&arg);
	return hw_map;
}

/*
 * _task_layout_lllp_cyclic
 *
 * task_layout_lllp_cyclic creates a cyclic distribution at the
 * lowest level of logical processor which is either socket, core or
 * thread depending on the system architecture. The Cyclic algorithm
 * is the same as the Cyclic distribution performed in srun.
 *
 *  Distribution at the lllp:
 *  -m hostfile|block|cyclic:block|cyclic
 *
 * The first distribution "hostfile|block|cyclic" is computed
 * in srun. The second distribution "block|cyclic" is computed
 * locally by each slurmd.
 *
 * The input to the lllp distribution algorithms is the gids (tasks
 * ids) generated for the local node.
 *
 * The output is a mapping of the gids onto logical processors
 * (thread/core/socket) with is expressed cpu_bind masks.
 *
 * If a task asks for more than one CPU per task, put the tasks as
 * close as possible (fill core rather than going next socket for the
 * extra task)
 *
 */
static int _task_layout_lllp_cyclic(launch_tasks_request_msg_t *req,
				    uint32_t node_id, bitstr_t ***masks_p)
{
	int last_taskcount = -1, taskcount = 0;
	uint16_t i, s, hw_sockets = 0, hw_cores = 0, hw_threads = 0;
	uint16_t offset = 0, p = 0;
	int size, max_tasks = req->tasks_to_launch[(int)node_id];
	int max_cpus = max_tasks * req->cpus_per_task;
	bitstr_t *avail_map;
	bitstr_t **masks = NULL;
	int *socket_last_pu = NULL;

	info ("_task_layout_lllp_cyclic ");

	avail_map = _get_avail_map(req, &hw_sockets, &hw_cores, &hw_threads);
	if (!avail_map)
		return SLURM_ERROR;

	//this means the mask was changed by DLB
	if(n_active_jobs != 0 && cpu_steal_infos[n_active_jobs-1]->job_id == req->job_id) 
		cpuset_to_bitstr(avail_map, cpu_steal_infos[n_active_jobs-1]->auto_mask, ncpus);

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
		     req->cpus_per_task, i);
		req->cpus_per_task = i;
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
				/* Switch to the next socket we have
				 * ran out here. */

				/* This only happens if the slurmctld
				 * gave us an allocation that made a
				 * task split sockets.  Or if the
				 * entire allocation is on one socket.
				 */
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
			if (req->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE)
				socket_last_pu[s] += hw_threads-1;

			if (!bit_test(avail_map, bit))
				continue;

			if (!masks[taskcount])
				masks[taskcount] =
					bit_alloc(conf->block_map_size);

			//info("setting %d %d", taskcount, bit);
			bit_set(masks[taskcount], bit);

			if (!already_switched &&
			    (((req->task_dist & SLURM_DIST_STATE_BASE) ==
			     SLURM_DIST_CYCLIC_CFULL) ||
			    ((req->task_dist & SLURM_DIST_STATE_BASE) ==
			     SLURM_DIST_BLOCK_CFULL))) {
				/* This means we are laying out cpus
				 * within a task cyclically as well. */
				s = (s + 1) % hw_sockets;
				already_switched = true;
			}

			if (++p < req->cpus_per_task)
				continue;

			/* Means we are binding to cores so skip the
			   rest of the threads in this one.  If a user
			   requests ntasks-per-core=1 and the
			   cpu_bind=threads this will not be able to
			   work since we don't know how many tasks per
			   core have been allocated.
			*/
			if (!(req->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE)
			    && ((req->cpu_bind_type & CPU_BIND_TO_CORES)
				|| (slurm_get_select_type_param() &
				    CR_ONE_TASK_PER_CORE))) {
				int threads_not_used;
				if (req->cpus_per_task < hw_threads)
					threads_not_used =
						hw_threads - req->cpus_per_task;
				else
					threads_not_used =
						req->cpus_per_task % hw_threads;
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

	/* last step: expand the masks to bind each task
	 * to the requested resource */
	_expand_masks(req->cpu_bind_type, max_tasks, masks,
		      hw_sockets, hw_cores, hw_threads, avail_map);
	FREE_NULL_BITMAP(avail_map);
	xfree(socket_last_pu);

	return SLURM_SUCCESS;
}

/*
 * _task_layout_lllp_block
 *
 * task_layout_lllp_block will create a block distribution at the
 * lowest level of logical processor which is either socket, core or
 * thread depending on the system architecture. The Block algorithm
 * is the same as the Block distribution performed in srun.
 *
 *  Distribution at the lllp:
 *  -m hostfile|plane|block|cyclic:block|cyclic
 *
 * The first distribution "hostfile|plane|block|cyclic" is computed
 * in srun. The second distribution "plane|block|cyclic" is computed
 * locally by each slurmd.
 *
 * The input to the lllp distribution algorithms is the gids (tasks
 * ids) generated for the local node.
 *
 * The output is a mapping of the gids onto logical processors
 * (thread/core/socket)  with is expressed cpu_bind masks.
 *
 */
static int _task_layout_lllp_block(launch_tasks_request_msg_t *req,
				   uint32_t node_id, bitstr_t ***masks_p)
{
	int c, i, size, last_taskcount = -1, taskcount = 0;
	uint16_t hw_sockets = 0, hw_cores = 0, hw_threads = 0;
	int max_tasks = req->tasks_to_launch[(int)node_id];
	int max_cpus = max_tasks * req->cpus_per_task;
	bitstr_t *avail_map;
	bitstr_t **masks = NULL;

	info("_task_layout_lllp_block ");

	avail_map = _get_avail_map(req, &hw_sockets, &hw_cores, &hw_threads);
	if (!avail_map) {
		return SLURM_ERROR;
	}
	//this means the mask was changed by DLB
        if(n_active_jobs != 0 && cpu_steal_infos[n_active_jobs-1]->job_id == req->job_id)
		cpuset_to_bitstr(avail_map, cpu_steal_infos[n_active_jobs-1]->auto_mask, ncpus);
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
		     req->cpus_per_task, i);
		req->cpus_per_task = i;
	}
	size = bit_size(avail_map);

	if ((req->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE) &&
	    (max_cpus > (hw_sockets * hw_cores))) {
		/* More CPUs requested than available cores,
		 * disable core-level binding */
		req->cpu_bind_type &= (~CPU_BIND_ONE_THREAD_PER_CORE);
	}

	*masks_p = xmalloc(max_tasks * sizeof(bitstr_t*));
	masks = *masks_p;

	/* block distribution with oversubsciption */
	c = 0;
	while (taskcount < max_tasks) {
		if (taskcount == last_taskcount) {
			fatal("_task_layout_lllp_block infinite loop");
		}
		last_taskcount = taskcount;
		/* the abstract map is already laid out in block order,
		 * so just iterate over it
		 */
		for (i = 0; i < size; i++) {
			/* skip unavailable resources */
			if (bit_test(avail_map, i) == 0)
				continue;

			if (!masks[taskcount])
				masks[taskcount] = bit_alloc(
					conf->block_map_size);
			//info("setting %d %d", taskcount, i);
			bit_set(masks[taskcount], i);

			/* skip unrequested threads */
			if (req->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE)
				i += hw_threads - 1;

			if (++c < req->cpus_per_task)
				continue;
			/* Means we are binding to cores so skip the
			   rest of the threads in this one.  If a user
			   requests ntasks-per-core=1 and the
			   cpu_bind=threads this will not be able to
			   work since we don't know how many tasks per
			   core have been allocated.
			*/
			if (!(req->cpu_bind_type & CPU_BIND_ONE_THREAD_PER_CORE)
			    && ((req->cpu_bind_type & CPU_BIND_TO_CORES)
				|| (slurm_get_select_type_param() &
				    CR_ONE_TASK_PER_CORE))) {
				int threads_not_used;
				if (req->cpus_per_task < hw_threads)
					threads_not_used =
						hw_threads - req->cpus_per_task;
				else
					threads_not_used =
						req->cpus_per_task % hw_threads;
				i += threads_not_used;
			}
			c = 0;
			if (++taskcount >= max_tasks)
				break;
		}
	}

	/* last step: expand the masks to bind each task
	 * to the requested resource */
	_expand_masks(req->cpu_bind_type, max_tasks, masks,
			hw_sockets, hw_cores, hw_threads, avail_map);
	FREE_NULL_BITMAP(avail_map);

	return SLURM_SUCCESS;
}

/*
 * _lllp_map_abstract_mask
 *
 * Map one abstract block mask to a physical machine mask
 *
 * IN - mask to map
 * OUT - mapped mask (storage allocated in this routine)
 */
static bitstr_t *_lllp_map_abstract_mask(bitstr_t *bitmask)
{
    	int i, bit;
	int num_bits = bit_size(bitmask);
	bitstr_t *newmask = NULL;
	newmask = (bitstr_t *) bit_alloc(num_bits);

	/* remap to physical machine */
	for (i = 0; i < num_bits; i++) {
		if (bit_test(bitmask,i)) {
			bit = BLOCK_MAP(i);
			if (bit < bit_size(newmask))
				bit_set(newmask, bit);
			else
				error("_lllp_map_abstract_mask: can't go from "
				      "%d -> %d since we only have %d bits",
				      i, bit, bit_size(newmask));
		}
	}
	return newmask;
}

/*
 * _lllp_map_abstract_masks
 *
 * Map an array of abstract block masks to physical machine masks
 *
 * IN- maximum number of tasks
 * IN/OUT- array of masks
 */
static void _lllp_map_abstract_masks(const uint32_t maxtasks, bitstr_t **masks)
{
    	int i;
	debug3("_lllp_map_abstract_masks");

	for (i = 0; i < maxtasks; i++) {
		bitstr_t *bitmask = masks[i];
	    	if (bitmask) {
			bitstr_t *newmask = _lllp_map_abstract_mask(bitmask);
			FREE_NULL_BITMAP(bitmask);
			masks[i] = newmask;
		}
	}
}

/*
 * _lllp_generate_cpu_bind
 *
 * Generate the cpu_bind type and string given an array of bitstr_t masks
 *
 * IN/OUT- job launch request (cpu_bind_type and cpu_bind updated)
 * IN- maximum number of tasks
 * IN- array of masks
 */
static void _lllp_generate_cpu_bind(launch_tasks_request_msg_t *req,
				    const uint32_t maxtasks, bitstr_t **masks)
{
    	int i, num_bits=0, masks_len;
	bitstr_t *bitmask;
	bitoff_t charsize;
	char *masks_str = NULL;
	char buf_type[100];

	for (i = 0; i < maxtasks; i++) {
		bitmask = masks[i];
	    	if (bitmask) {
			num_bits = bit_size(bitmask);
			break;
		}
	}
	charsize = (num_bits + 3) / 4;		/* ASCII hex digits */
	charsize += 3;				/* "0x" and trailing "," */
	masks_len = maxtasks * charsize + 1;	/* number of masks + null */

	debug3("_lllp_generate_cpu_bind %d %d %d", maxtasks, charsize,
		masks_len);

	masks_str = xmalloc(masks_len);
	masks_len = 0;
	for (i = 0; i < maxtasks; i++) {
	    	char *str;
		int curlen;
		bitmask = masks[i];
	    	if (bitmask == NULL) {
			continue;
		}
		str = (char *)bit_fmt_hexmask(bitmask);
		curlen = strlen(str) + 1;

		if (masks_len > 0)
			masks_str[masks_len-1]=',';
		strncpy(&masks_str[masks_len], str, curlen);
		masks_len += curlen;
		xassert(masks_str[masks_len] == '\0');
		xfree(str);
	}

	if (req->cpu_bind) {
	    	xfree(req->cpu_bind);
	}
	if (masks_str[0] != '\0') {
		req->cpu_bind = masks_str;
		req->cpu_bind_type |= CPU_BIND_MASK;
	} else {
		req->cpu_bind = NULL;
		req->cpu_bind_type &= ~CPU_BIND_VERBOSE;
	}

	/* clear mask generation bits */
	req->cpu_bind_type &= ~CPU_BIND_TO_THREADS;
	req->cpu_bind_type &= ~CPU_BIND_TO_CORES;
	req->cpu_bind_type &= ~CPU_BIND_TO_SOCKETS;
	req->cpu_bind_type &= ~CPU_BIND_TO_LDOMS;

	slurm_sprint_cpu_bind_type(buf_type, req->cpu_bind_type);
	info("_lllp_generate_cpu_bind jobid [%u]: %s, %s",
	     req->job_id, buf_type, masks_str);
}
