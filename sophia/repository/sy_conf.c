
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
#include <libsd.h>
#include <libsy.h>

void sy_confinit(syconf *c)
{
	c->path        = NULL;
	c->path_backup = NULL;
	c->sync        = 0;
}

void sy_conffree(syconf *c, ssa *a)
{
	if (c->path)
		ss_free(a, c->path);
	if (c->path_backup)
		ss_free(a, c->path_backup);
}
