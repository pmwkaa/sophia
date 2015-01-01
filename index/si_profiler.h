#ifndef SI_PROFILER_H_
#define SI_PROFILER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct siprofiler siprofiler;

struct siprofiler {
	si *i;
	uint32_t  total_node_count;
	uint64_t  total_node_size;
	uint32_t  total_branch_count;
	uint32_t  total_branch_avg;
	uint32_t  total_branch_max;
	uint64_t  memory_used;
	uint64_t  count;
	uint64_t  count_dup;
	uint64_t  read_disk;
	uint64_t  read_cache;
	int       histogram_branch[20];
	int       histogram_branch_20plus;
	char      histogram_branch_sz[512];
	char     *histogram_branch_ptr;
	srseq     seq;
} srpacked;

int si_profilerbegin(siprofiler*, si*);
int si_profilerend(siprofiler*);
int si_profiler(siprofiler*, sr*);

#endif
