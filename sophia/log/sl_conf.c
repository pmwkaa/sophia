
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
#include <libsl.h>

void sl_confinit(slconf *c)
{
	c->enable         = 1;
	c->path           = NULL;
	c->rotatewm       = 500000;
	c->sync_on_write  = 0;
	c->sync_on_rotate = 1;
}

void sl_conffree(slconf *c, ssa *a)
{
	if (c->path)
		ss_free(a, c->path);
}

int sl_confset_path(slconf *c, ssa *a, char *path)
{
	char *sz = ss_strdup(a, path);
	if (ssunlikely(sz == NULL))
		return -1;
	if (c->path)
		ss_free(a, c->path);
	c->path = sz;
	return 0;
}
