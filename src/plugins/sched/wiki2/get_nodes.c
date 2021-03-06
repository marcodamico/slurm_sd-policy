/*****************************************************************************\
 *  get_nodes.c - Process Wiki get node info request
 *****************************************************************************
 *  Copyright (C) 2006-2007 The Regents of the University of California.
 *  Copyright (C) 2008-2010 Lawrence Livermore National Security.
 *  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
 *  Written by Morris Jette <jette1@llnl.gov>
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

#include "./msg.h"
#include "src/common/hostlist.h"
#include "src/common/node_select.h"
#include "src/slurmctld/locks.h"
#include "src/slurmctld/slurmctld.h"

static char *	_dump_all_nodes(int *node_cnt, time_t update_time);
static char *	_dump_node(struct node_record *node_ptr, hostlist_t hl,
			   time_t update_time);
static bool	_hidden_node(struct node_record *node_ptr);
static char *	_get_node_state(struct node_record *node_ptr);
static int	_same_info(struct node_record *node1_ptr,
			   struct node_record *node2_ptr, time_t update_time);
static int	_str_cmp(char *s1, char *s2);

/*
 * get_nodes - get information on specific node(s) changed since some time
 * cmd_ptr IN   - CMD=GETNODES ARG=[<UPDATETIME>:<NODEID>[:<NODEID>]...]
 *                               [<UPDATETIME>:ALL]
 * err_code OUT - 0 or an error code
 * err_msg OUT  - response message
 * NOTE: xfree() err_msg if err_code is zero
 * RET 0 on success, -1 on failure
 *
 * Response format
 * ARG=<cnt>#<NODEID>:
 *	STATE=<state>;		 Moab equivalent node state
 *	[CAT=<reason>];		 Reason for a node being down or drained
 *				 colon separator
 *	CCLASS=<[part:cpus]>;	 SLURM partition with CPU count of node,
 *				 make have more than one partition
 *	[CPULOAD=<load_ave>;]	 One minute BSD load average
 *	[ARCH=<architecture>;]	 Computer architecture
 *	[OS=<operating_system>;] Operating system
 *	CMEMORY=<MB>;		 MB of memory on node
 *	CDISK=<MB>;		 MB of disk space on node
 *	CPROC=<cpus>;		 CPU count on node
 *	[FEATURE=<feature>;]	 Features associated with node, if any
 *	[GRES=<name>[:<count>],...;] generic resources on the node
 *  [#<NODEID>:...];
 */
extern int	get_nodes(char *cmd_ptr, int *err_code, char **err_msg)
{
	char *arg_ptr = NULL, *tmp_char = NULL, *tmp_buf = NULL, *buf = NULL;
	time_t update_time;
	/* Locks: read node, read partition */
	slurmctld_lock_t node_read_lock = {
		NO_LOCK, NO_LOCK, READ_LOCK, READ_LOCK };
	int node_rec_cnt = 0, buf_size = 0;

#ifdef HAVE_ALPS_CRAY
	/* Locks: write node */
	slurmctld_lock_t node_write_lock = {
		NO_LOCK, NO_LOCK, WRITE_LOCK, NO_LOCK };

	/*
	 * Run a Basil Inventory immediately before scheduling, to avoid
	 * race conditions caused by ALPS node state change (caused e.g.
	 * by the node health checker).
	 * This relies on the above write lock for the node state.
	 */
	lock_slurmctld(node_write_lock);
	if (select_g_update_block(NULL)) {
		unlock_slurmctld(node_write_lock);
		*err_code = -720;
		*err_msg = "Unable to run ALPS inventory";
		error("wiki: Unable to run ALPS inventory");
		return -1;
	}
	unlock_slurmctld(node_write_lock);
#endif

	arg_ptr = strstr(cmd_ptr, "ARG=");
	if (arg_ptr == NULL) {
		*err_code = -300;
		*err_msg = "GETNODES lacks ARG";
		error("wiki: GETNODES lacks ARG");
		return -1;
	}
	update_time = (time_t) strtoul(arg_ptr+4, &tmp_char, 10);
	if (tmp_char[0] != ':') {
		*err_code = -300;
		*err_msg = "Invalid ARG value";
		error("wiki: GETNODES has invalid ARG value");
		return -1;
	}
	tmp_char++;
	lock_slurmctld(node_read_lock);
	if (strncmp(tmp_char, "ALL", 3) == 0) {
		/* report all nodes */
		buf = _dump_all_nodes(&node_rec_cnt, update_time);
	} else {
		struct node_record *node_ptr = NULL;
		char *node_name, *slurm_hosts;
		int node_cnt;
		hostset_t slurm_hostset;

		slurm_hosts = moab2slurm_task_list(tmp_char, &node_cnt);
		if ((slurm_hostset = hostset_create(slurm_hosts))) {
			while ((node_name = hostset_shift(slurm_hostset))) {
				node_ptr = find_node_record(node_name);
				if (node_ptr == NULL) {
					error("sched/wiki2: bad hostname %s",
					      node_name);
					continue;
				}
				if (_hidden_node(node_ptr))
					continue;
				tmp_buf = _dump_node(node_ptr, NULL,
						     update_time);
				if (node_rec_cnt > 0)
					xstrcat(buf, "#");
				xstrcat(buf, tmp_buf);
				xfree(tmp_buf);
				node_rec_cnt++;
			}
			hostset_destroy(slurm_hostset);
		} else {
			error("hostset_create(%s): %m", slurm_hosts);
		}
		xfree(slurm_hosts);
	}
	unlock_slurmctld(node_read_lock);

	/* Prepend ("ARG=%d", node_rec_cnt) to reply message */
	if (buf)
		buf_size = strlen(buf);
	tmp_buf = xmalloc(buf_size + 32);
	if (node_rec_cnt)
		sprintf(tmp_buf, "SC=0 ARG=%d#%s", node_rec_cnt, buf);
	else
		sprintf(tmp_buf, "SC=0 ARG=0#");
	xfree(buf);
	*err_code = 0;
	*err_msg = tmp_buf;
	return 0;
}

static char *	_dump_all_nodes(int *node_cnt, time_t update_time)
{
	int i, cnt = 0, rc;
	struct node_record *node_ptr = node_record_table_ptr;
	char *tmp_buf = NULL, *buf = NULL;
	struct node_record *uniq_node_ptr = NULL;
	hostlist_t hl = NULL;

	for (i=0; i<node_record_count; i++, node_ptr++) {
		if (node_ptr->name == NULL)
			continue;
		if (IS_NODE_FUTURE(node_ptr))
			continue;
		if (_hidden_node(node_ptr))
			continue;
		if (use_host_exp == 2) {
			rc = _same_info(uniq_node_ptr, node_ptr, update_time);
			if (rc == 0) {
				uniq_node_ptr = node_ptr;
				if (hl) {
					hostlist_push_host(hl, node_ptr->name);
				} else {
					hl = hostlist_create(node_ptr->name);
					if (!hl) {
						fatal("Invalid node_name: %s",
						      node_ptr->name);
					}
				}
				continue;
			} else {
				tmp_buf = _dump_node(uniq_node_ptr, hl,
						     update_time);
				hostlist_destroy(hl);
				hl = hostlist_create(node_ptr->name);
				if (!hl) {
					fatal("Invalid node_name: %s",
					      node_ptr->name);
				}
				uniq_node_ptr = node_ptr;
			}
		} else {
			tmp_buf = _dump_node(node_ptr, hl, update_time);
		}
		if (cnt > 0)
			xstrcat(buf, "#");
		xstrcat(buf, tmp_buf);
		xfree(tmp_buf);
		cnt++;
	}

	if (hl) {
		tmp_buf = _dump_node(uniq_node_ptr, hl, update_time);
		hostlist_destroy(hl);
		if (cnt > 0)
			xstrcat(buf, "#");
		xstrcat(buf, tmp_buf);
		xfree(tmp_buf);
		cnt++;
	}

	*node_cnt = cnt;
	return buf;
}

/* Determine if node1 and node2 have the same parameters that we report to Moab
 * RET 0 of node1 is NULL or their parameters are the same
 *     >0 otherwise
 */
static int _same_info(struct node_record *node1_ptr,
		      struct node_record *node2_ptr, time_t update_time)
{
	int i;

	if (node1_ptr == NULL)	/* first record, treat as a match */
		return 0;

	if (node1_ptr->node_state != node2_ptr->node_state)
		return 1;
	if (_str_cmp(node1_ptr->reason, node2_ptr->reason))
		return 2;
	if (update_time > last_node_update)
		return 0;

	if (slurmctld_conf.fast_schedule) {
		/* config from slurm.conf */
		if (node1_ptr->config_ptr->cpus != node2_ptr->config_ptr->cpus)
			return 3;
	} else {
		/* config as reported by slurmd */
		if (node1_ptr->cpus != node2_ptr->cpus)
			return 4;
	}
	if (node1_ptr->part_cnt != node2_ptr->part_cnt)
		return 5;
	for (i=0; i<node1_ptr->part_cnt; i++) {
		if (node1_ptr->part_pptr[i] !=  node2_ptr->part_pptr[i])
			return 6;
	}
	if (_str_cmp(node1_ptr->arch, node2_ptr->arch))
		return 7;
	if (_str_cmp(node1_ptr->os, node2_ptr->os))
		return 8;
	if (update_time > 0)
		return 0;

	if (slurmctld_conf.fast_schedule) {
		/* config from slurm.conf */
		if ((node1_ptr->config_ptr->real_memory !=
		     node2_ptr->config_ptr->real_memory) ||
		    (node1_ptr->config_ptr->tmp_disk !=
		     node2_ptr->config_ptr->tmp_disk) ||
		    (node1_ptr->config_ptr->cpus !=
		     node2_ptr->config_ptr->cpus))
			return 9;
	} else {
		if ((node1_ptr->real_memory != node2_ptr->real_memory) ||
		    (node1_ptr->tmp_disk    != node2_ptr->tmp_disk) ||
		    (node1_ptr->cpus        != node2_ptr->cpus))
			return 10;
	}
	if (_str_cmp(node1_ptr->config_ptr->feature,
		     node2_ptr->config_ptr->feature))
		return 11;

	if (_str_cmp(node1_ptr->config_ptr->gres,
		     node2_ptr->config_ptr->gres))
		return 12;

	return 0;
}

static char *	_dump_node(struct node_record *node_ptr, hostlist_t hl,
			   time_t update_time)
{
	char tmp[16*1024], *buf = NULL;
	int i;
	uint32_t cpu_cnt;

	if (!node_ptr)
		return NULL;

	if (hl) {
		char *node_list;
		hostlist_sort(hl);
		hostlist_uniq(hl);
		node_list = hostlist_ranged_string_xmalloc(hl);
		xstrcat(buf, node_list);
		xfree(node_list);
	} else {
		snprintf(tmp, sizeof(tmp), "%s", node_ptr->name);
		xstrcat(buf, tmp);
	}

	snprintf(tmp, sizeof(tmp), ":STATE=%s;", _get_node_state(node_ptr));
	xstrcat(buf, tmp);

	if (node_ptr->cpu_load != NO_VAL) {
		snprintf(tmp, sizeof(tmp), "CPULOAD=%f;",
			 (node_ptr->cpu_load / 100.0));
		xstrcat(buf, tmp);
	}

	if (node_ptr->reason) {
		/* Strip out any quotes, they confuse Moab */
		char *reason, *bad_char;
		reason = xstrdup(node_ptr->reason);
		while ((bad_char = strchr(reason, '\'')))
			bad_char[0] = ' ';
		while ((bad_char = strchr(reason, '\"')))
			bad_char[0] = ' ';
		snprintf(tmp, sizeof(tmp), "CAT=\"%s\";", reason);
		xstrcat(buf, tmp);
		xfree(reason);
	}

	if (update_time > last_node_update)
		return buf;

	if (slurmctld_conf.fast_schedule) {
		/* config from slurm.conf */
		cpu_cnt = node_ptr->config_ptr->cpus;
	} else {
		/* config as reported by slurmd */
		cpu_cnt = node_ptr->cpus;
	}
	for (i=0; i<node_ptr->part_cnt; i++) {
		if (i == 0)
			xstrcat(buf, "CCLASS=");
		snprintf(tmp, sizeof(tmp), "[%s:%u]",
			node_ptr->part_pptr[i]->name,
			cpu_cnt);
		xstrcat(buf, tmp);
	}
	if (i > 0)
		xstrcat(buf, ";");

	if (node_ptr->arch) {
		snprintf(tmp, sizeof(tmp), "ARCH=%s;", node_ptr->arch);
		xstrcat(buf, tmp);
	}

	if (node_ptr->os) {
		snprintf(tmp, sizeof(tmp), "OS=%s;", node_ptr->os);
		xstrcat(buf, tmp);
	}

	if (node_ptr->config_ptr && node_ptr->config_ptr->feature) {
		snprintf(tmp, sizeof(tmp), "FEATURE=%s;",
			node_ptr->config_ptr->feature);
		/* comma separator to colon */
		for (i=0; (tmp[i] != '\0'); i++) {
			if (tmp[i] == ',')
				tmp[i] = ':';
		}
		xstrcat(buf, tmp);
	}

	if (node_ptr->config_ptr && node_ptr->config_ptr->gres) {
		snprintf(tmp, sizeof(tmp), "GRES=%s;",
			node_ptr->config_ptr->gres);
		xstrcat(buf, tmp);
	}

	if (update_time > 0)
		return buf;

	if (slurmctld_conf.fast_schedule && node_ptr->config_ptr) {
		/* config from slurm.conf */
		snprintf(tmp, sizeof(tmp),
			"CMEMORY=%u;CDISK=%u;CPROC=%u;",
			node_ptr->config_ptr->real_memory,
			node_ptr->config_ptr->tmp_disk,
			node_ptr->config_ptr->cpus);
	} else {
		/* config as reported by slurmd */
		snprintf(tmp, sizeof(tmp),
			"CMEMORY=%u;CDISK=%u;CPROC=%u;",
			node_ptr->real_memory,
			node_ptr->tmp_disk,
			node_ptr->cpus);
	}
	xstrcat(buf, tmp);

	return buf;
}

static char *	_get_node_state(struct node_record *node_ptr)
{
	static bool got_select_type = false;
	static bool node_allocations;

	if (!got_select_type) {
		char * select_type = slurm_get_select_type();
		if (select_type &&
		    (strcasecmp(select_type, "select/linear") == 0))
			node_allocations = true;
		else
			node_allocations = false;
		xfree(select_type);
		got_select_type = true;
	}

	if (IS_NODE_DRAIN(node_ptr) || IS_NODE_FAIL(node_ptr)) {
			return "Drained";
		return "Draining";
	}
	if (IS_NODE_COMPLETING(node_ptr))
		return "Busy";

	if (IS_NODE_DOWN(node_ptr))
		return "Down";
	if (IS_NODE_ALLOCATED(node_ptr)) {
		if (node_allocations)
			return "Busy";
		else
			return "Running";
	}
	if (IS_NODE_IDLE(node_ptr))
		return "Idle";

	return "Unknown";
}

/* Like strcmp(), but can handle NULL pointers */
static int	_str_cmp(char *s1, char *s2)
{
	if (s1 && s2)
		return strcmp(s1, s2);

	if ((s1 == NULL) && (s2 == NULL))
		return 0;

	/* One pointer is valid and the other is NULL */
	return 1;
}

/* Return true if the node exists in a hidden partition and not in any
 * non-hidden partitions. */
static bool	_hidden_node(struct node_record *node_ptr)
{
	int i, n;
	int hidden = -1;	/* node is hidden for some partition */
	int shown = -1;		/* node is *not* hidden for some partition */

	for (n = 0; n < node_ptr->part_cnt; n++) {
		bool hide_found = false;
		for (i=0; i<HIDE_PART_CNT; i++) {
			if (hide_part_nodes_ptr[i] == NULL)
				break;
			if (hide_part_nodes_ptr[i] == node_ptr->part_pptr[n]) {
				hide_found = true;
				break;
			}
		}
		if (hide_found)
			hidden = 1;
		else
			shown = 1;
	}

	if ((hidden == 1) && (shown != 1))
		return true;
	return false;
}
