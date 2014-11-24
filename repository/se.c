
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libse.h>

int se_init(se *e)
{
	e->conf = NULL;
	return 0;
}

static int
se_deploy(se *e, sr *r)
{
	int rc;
	rc = sr_filemkdir(e->conf->path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "directory '%s' create error: %s",
		         e->conf->path, strerror(errno));
		return -1;
	}
	return 0;
}

static int
se_recover(se *e)
{
	(void)e;
	return 0;
}

int se_open(se *e, sr *r, seconf *conf)
{
	e->conf = conf;
	int exists = sr_fileexists(conf->path);
	if (exists == 0)
		return se_deploy(e, r);
	return se_recover(e);
}

int se_close(se *e)
{
	(void)e;
	return 0;
}
