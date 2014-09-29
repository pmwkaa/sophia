
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

int so_dbctl_init(sodbctl *c, char *name, void *db)
{
	memset(c, 0, sizeof(*c));
	sodb *o = db;
	c->name = sr_strdup(&o->e->a, name);
	if (srunlikely(c->name == NULL))
		return -1;
	c->parent           = db;
	c->logdir_create    = 1;
	c->logdir_write     = 1;
	c->logdir_rotate_wm = 500000;
	c->dir_create       = 1;
	c->dir_write        = 1;
	c->cmp.cmp          = sr_cmpstring;
	c->cmp.cmparg       = NULL;
	c->memory_limit     = 0;
	c->node_size        = 128 * 1024 * 1024; 
	c->node_page_size   = 128 * 1024;
	c->node_branch_wm   = 10 * 1024 * 1024;
	c->node_merge_wm    = 1;
	c->threads_merge    = 2;
	c->threads_branch   = 1;
	c->threads          = 3;
	return 0;
}

int so_dbctl_free(sodbctl *c)
{
	sodb *o = c->parent;
	if (so_dbactive(o))
		return -1;
	if (c->name) {
		sr_free(&o->e->a, c->name);
		c->name = NULL;
	}
	if (c->dir) {
		sr_free(&o->e->a, c->dir);
		c->dir = NULL;
	}
	if (c->logdir) {
		sr_free(&o->e->a, c->logdir);
		c->logdir = NULL;
	}
	return 0;
}

int so_dbctl_validate(sodbctl *c)
{
	if (c->dir == NULL)
		return -1;
	return 0;
}

int so_dbctl_set(sodbctl *c, char *path, va_list args)
{
	sodb *db = c->parent;
	if (so_dbactive(db))
		return -1;

	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL))
		return 0; /* db create */

	if (strcmp(token, "name") == 0) {
		return -1;
	} else
	if (strcmp(token, "dir") == 0) {
		char *p = sr_strdup(&db->e->a, va_arg(args, char*));
		if (srunlikely(p == NULL))
			return -1;
		if (c->dir)
			sr_free(&db->e->a, c->dir);
		c->dir = p;
	} else
	if (strcmp(token, "dir_read") == 0) {
		c->dir_read = va_arg(args, int);
	} else
	if (strcmp(token, "dir_write") == 0) {
		c->dir_write = va_arg(args, int);
	} else
	if (strcmp(token, "dir_create") == 0) {
		c->dir_create = va_arg(args, int);
	} else
	if (strcmp(token, "logdir") == 0) {
		char *p = sr_strdup(&db->e->a, va_arg(args, char*));
		if (srunlikely(p == NULL))
			return -1;
		if (c->logdir)
			sr_free(&db->e->a, c->logdir);
		c->logdir = p;
	} else
	if (strcmp(token, "logdir_read") == 0) {
		c->logdir_read = va_arg(args, int);
	} else
	if (strcmp(token, "logdir_write") == 0) {
		c->logdir_write = va_arg(args, int);
	} else
	if (strcmp(token, "logdir_create") == 0) {
		c->logdir_create = va_arg(args, int);
	} else
	if (strcmp(token, "cmp") == 0) {
		c->cmp.cmp = va_arg(args, srcmpf);
	} else
	if (strcmp(token, "cmp_arg") == 0) {
		c->cmp.cmparg = va_arg(args, void*);
	} else
	if (strcmp(token, "node_size") == 0) {
		c->node_size = va_arg(args, int);
	} else
	if (strcmp(token, "node_page_size") == 0) {
		c->node_page_size = va_arg(args, int);
	} else
	if (strcmp(token, "node_branch_wm") == 0) {
		c->node_branch_wm = va_arg(args, int);
	} else
	if (strcmp(token, "node_merge_wm") == 0) {
		c->node_merge_wm = va_arg(args, int);
	} else
	if (strcmp(token, "threads") == 0) {
		c->threads = va_arg(args, int);
	} else
	if (strcmp(token, "memory_limit") == 0) {
		c->memory_limit = va_arg(args, uint64_t);
	} else
	if (strcmp(token, "branch") == 0) {
		sdc dc;
		sd_cinit(&dc, &db->r);
		int rc;
		while (1) {
			rc = si_branch(&db->index, &db->r, &dc, db->ctl.node_branch_wm);
			if (srunlikely(rc <= 0))
				break;
		}
		sd_cfree(&dc, &db->r);
		return rc;
	} else
	if (strcmp(token, "merge") == 0) {
		sdc dc;
		sd_cinit(&dc, &db->r);
		int rc;
		while (1) {
			rc = si_merge(&db->index, &db->r, &dc, db->ctl.node_merge_wm);
			if (srunlikely(rc <= 0))
				break;
		}
		sd_cfree(&dc, &db->r);
		return rc;
	} else
	if (strcmp(token, "logrotate") == 0) {
		return sl_poolrotate(&db->lp);
	} else {
		return -1;
	}
	return 0;
}

/*
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
*/

void *so_dbctl_get(sodbctl *c, char *path, va_list args)
{
	sodb *db = c->parent;
	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL))
		return &db->o;

	(void)c;
	(void)path;
	(void)args;
	return NULL;
}
