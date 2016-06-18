
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

int sy_init(sy *e)
{
	sy_confinit(&e->conf);
	return 0;
}

static int
sy_deploy(sy *e, sr *r)
{
	int rc;
	rc = ss_vfsmkdir(r->vfs, e->conf.path, 0755);
	if (ssunlikely(rc == -1)) {
		sr_error(r->e, "directory '%s' create error: %s",
		         e->conf.path, strerror(errno));
		return -1;
	}
	return 0;
}

static inline ssize_t
sy_processid(char **str) {
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
sy_process(char *name, uint32_t *bsn)
{
	/* id */
	/* id.incomplete */
	char *token = name;
	ssize_t id = sy_processid(&token);
	if (ssunlikely(id == -1))
		return -1;
	*bsn = id;
	if (strcmp(token, ".incomplete") == 0)
		return 1;
	return 0;
}

static inline int
sy_recoverbackup(sy *i, sr *r)
{
	if (i->conf.path_backup == NULL)
		return 0;
	int rc;
	int exists = ss_vfsexists(r->vfs, i->conf.path_backup);
	if (! exists) {
		rc = ss_vfsmkdir(r->vfs, i->conf.path_backup, 0755);
		if (ssunlikely(rc == -1)) {
			sr_error(r->e, "backup directory '%s' create error: %s",
					 i->conf.path_backup, strerror(errno));
			return -1;
		}
	}
	/* recover backup sequential number */
	DIR *dir = opendir(i->conf.path_backup);
	if (ssunlikely(dir == NULL)) {
		sr_error(r->e, "backup directory '%s' open error: %s",
				 i->conf.path_backup, strerror(errno));
		return -1;
	}
	uint32_t bsn = 0;
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (ssunlikely(de->d_name[0] == '.'))
			continue;
		uint32_t id = 0;
		rc = sy_process(de->d_name, &id);
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

int sy_open(sy *e, sr *r)
{
	int rc = sy_recoverbackup(e, r);
	if (ssunlikely(rc == -1))
		return -1;
	int exists = ss_vfsexists(r->vfs, e->conf.path);
	if (exists == 0)
		return sy_deploy(e, r);
	return 0;
}

int sy_close(sy *e, sr *r)
{
	sy_conffree(&e->conf, r->a);
	return 0;
}
