
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsw.h>

void sw_confinit(swconf *c)
{
	c->enable         = 1;
	c->path           = NULL;
	c->rotatewm       = 500000;
	c->sync_on_write  = 0;
	c->sync_on_rotate = 1;
}

void sw_conffree(swconf *c, ssa *a)
{
	if (c->path)
		ss_free(a, c->path);
}

int sw_confset_path(swconf *c, ssa *a, char *path)
{
	char *sz = ss_strdup(a, path);
	if (ssunlikely(sz == NULL))
		return -1;
	if (c->path)
		ss_free(a, c->path);
	c->path = sz;
	return 0;
}
