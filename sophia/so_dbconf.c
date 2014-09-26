
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

static int
so_dbconf_exec(sodbconf *c, sra *a, char *path, va_list args)
{
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL))
		return -1;
	if (strcmp(token, "storage") != 0)
		return -1;
	token = strtok_r(NULL, ".", &ptr);
	if (srunlikely(token == NULL))
		return -1;
	if (strcmp(token, "dir") == 0) {
		char *p = sr_strdup(a, va_arg(args, char*));
		if (srunlikely(p == NULL))
			return -1;
		if (c->dir)
			sr_free(a, c->dir);
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
		char *p = sr_strdup(a, va_arg(args, char*));
		if (srunlikely(p == NULL))
			return -1;
		if (c->logdir)
			sr_free(a, c->logdir);
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
		return -1;
	return 0;
}

static int
so_dbconf_set(soobj *obj, va_list args)
{
	sodbconf *c = (sodbconf*)obj;
	sodb *o = c->parent;
	if (so_dbactive(o))
		return -1;
	char *path = va_arg(args, char*);
	return so_dbconf_exec(c, &o->e->a, path, args);
}

static int
so_dbconf_destroy(soobj *obj)
{
	sodbconf *c = (sodbconf*)obj;
	sodb *o = c->parent;
	if (so_dbactive(o))
		return 0;
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

static soobjif sodbconfif =
{
	.ctl      = NULL,
	.storage  = NULL,
	.open     = NULL,
	.destroy  = so_dbconf_destroy,
	.set      = so_dbconf_set,
	.get      = NULL,
	.del      = NULL,
	.begin    = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.backup   = NULL,
	.object   = NULL,
	.type     = NULL,
	.copy     = NULL
};

void so_dbconf_init(sodbconf *c, void *o)
{
	memset(c, 0, sizeof(*c));
	so_objinit(&c->o, SODBCONF, &sodbconfif);
	c->parent = o;
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
}

int so_dbconf_validate(sodbconf *c)
{
	if (c->dir == NULL)
		return -1;
	return 0;
}
