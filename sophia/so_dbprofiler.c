
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

static void*
so_dbprofiler_get(soobj *obj, va_list args)
{
	sodbprofiler *p = (sodbprofiler*)obj;
	sodb *db = p->db;
	si_profilerbegin(&p->prof, &db->index);
	si_profiler(&p->prof);
	si_profilerend(&p->prof);
	char *name = va_arg(args, char*);
	int  *size = va_arg(args, int*);
	if (strcmp(name, "count") == 0) {
		if (size)
			*size = sizeof(p->prof.count);
		return &p->prof.count;
	} else
	if (strcmp(name, "total_node_count") == 0) {
		if (size)
			*size = sizeof(p->prof.total_node_count);
		return &p->prof.total_node_count;
	} else
	if (strcmp(name, "total_node_size") == 0) {
		if (size)
			*size = sizeof(p->prof.total_node_size);
		return &p->prof.total_node_size;
	} else
	if (strcmp(name, "total_branch_count") == 0) {
		if (size)
			*size = sizeof(p->prof.total_branch_count);
		return &p->prof.total_branch_count;
	} else
	if (strcmp(name, "total_branch_max") == 0) {
		if (size)
			*size = sizeof(p->prof.total_branch_max);
		return &p->prof.total_branch_max;
	} else
	if (strcmp(name, "total_branch_size") == 0) {
		if (size)
			*size = sizeof(p->prof.total_branch_size);
		return &p->prof.total_branch_size;
	} else
	if (strcmp(name, "memory_used") == 0) {
		if (size)
			*size = sizeof(p->prof.memory_used);
		return &p->prof.memory_used;
	} else
	if (strcmp(name, "count") == 0) {
		if (size)
			*size = sizeof(p->prof.count);
		return &p->prof.count;
	}
	return NULL;
}

static void*
so_dbprofiler_type(soobj *o srunused, va_list args srunused) {
	return "db_profiler";
}

static soobjif sodbprofilerif =
{
	.ctl      = NULL,
	.storage  = NULL,
	.open     = NULL,
	.destroy  = NULL, 
	.set      = NULL,
	.get      = so_dbprofiler_get,
	.del      = NULL,
	.begin    = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.backup   = NULL,
	.object   = NULL,
	.type     = so_dbprofiler_type,
	.copy     = NULL
};

void so_dbprofiler_init(sodbprofiler *p, void *db)
{
	so_objinit(&p->o, SODBPROFILER, &sodbprofilerif);
	p->db = db;
}
