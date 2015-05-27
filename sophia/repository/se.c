
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
	rc = ss_filemkdir(e->conf->path);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "directory '%s' create error: %s",
		         e->conf->path, strerror(errno));
		return -1;
	}
	return 0;
}

static inline int
se_recover(se *e, sr *r)
{
	(void)e;
	(void)r;
	return 0;
}

static inline ssize_t
se_processid(char **str) {
	char *s = *str;
	size_t v = 0;
	while (*s && *s != '.') {
		if (ssunlikely(!isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	*str = s;
	return v;
}

static inline int
se_process(char *name, uint32_t *bsn)
{
	/* id */
	/* id.incomplete */
	char *token = name;
	ssize_t id = se_processid(&token);
	if (ssunlikely(id == -1))
		return -1;
	*bsn = id;
	if (strcmp(token, ".incomplete") == 0)
		return 1;
	return 0;
}

static inline int
se_recoverbackup(se *i, sr *r)
{
	if (i->conf->path_backup == NULL)
		return 0;
	int rc;
	int exists = ss_fileexists(i->conf->path_backup);
	if (! exists) {
		rc = ss_filemkdir(i->conf->path_backup);
		if (ssunlikely(rc == -1)) {
			sr_error(r->e, "backup directory '%s' create error: %s",
					 i->conf->path_backup, strerror(errno));
			return -1;
		}
	}
	/* recover backup sequential number */
	DIR *dir = opendir(i->conf->path_backup);
	if (ssunlikely(dir == NULL)) {
		sr_error(r->e, "backup directory '%s' open error: %s",
				 i->conf->path_backup, strerror(errno));
		return -1;
	}
	uint32_t bsn = 0;
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (ssunlikely(de->d_name[0] == '.'))
			continue;
		uint32_t id = 0;
		rc = se_process(de->d_name, &id);
		switch (rc) {
		case  1:
		case  0:
			if (id > bsn)
				bsn = id;
			break;
		case -1: /* skip unknown file */
			continue;
		}
	}
	closedir(dir);
	r->seq->bsn = bsn;
	return 0;
}

int se_open(se *e, sr *r, seconf *conf)
{
	e->conf = conf;
	int rc = se_recoverbackup(e, r);
	if (ssunlikely(rc == -1))
		return -1;
	int exists = ss_fileexists(conf->path);
	if (exists == 0) {
		if (ssunlikely(! conf->path_create)) {
			sr_error(r->e, "directory '%s' does not exist", conf->path);
			return -1;
		}
		return se_deploy(e, r);
	}
	return se_recover(e, r);
}

int se_close(se *e, sr *r)
{
	(void)e;
	(void)r;
	return 0;
}
