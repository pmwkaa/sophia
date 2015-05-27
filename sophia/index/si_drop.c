
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
#include <libsi.h>

static inline int
si_dropof(sischeme *scheme, sr *r)
{
	DIR *dir = opendir(scheme->path);
	if (dir == NULL) {
		sr_malfunction(r->e, "directory '%s' open error: %s",
		               scheme->path, strerror(errno));
		return -1;
	}
	char path[1024];
	int rc;
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (de->d_name[0] == '.')
			continue;
		/* skip drop file */
		if (ssunlikely(strcmp(de->d_name, "drop") == 0))
			continue;
		snprintf(path, sizeof(path), "%s/%s", scheme->path, de->d_name);
		rc = ss_fileunlink(path);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "db file '%s' unlink error: %s",
			               path, strerror(errno));
			closedir(dir);
			return -1;
		}
	}
	closedir(dir);

	snprintf(path, sizeof(path), "%s/drop", scheme->path);
	rc = ss_fileunlink(path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' unlink error: %s",
		               path, strerror(errno));
		return -1;
	}
	rc = rmdir(scheme->path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "directory '%s' unlink error: %s",
		               scheme->path, strerror(errno));
		return -1;
	}
	return 0;
}

int si_dropmark(si *i, sr *r)
{
	/* create drop file */
	char path[1024];
	snprintf(path, sizeof(path), "%s/drop", i->scheme->path);
	ssfile drop;
	ss_fileinit(&drop, r->a);
	int rc = ss_filenew(&drop, path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "drop file '%s' create error: %s",
		               path, strerror(errno));
		return -1;
	}
	ss_fileclose(&drop);
	return 0;
}

int si_drop(si *i, sr *r)
{
	sischeme *scheme = i->scheme;
	/* drop file must exists at this point */
	/* shutdown */
	int rc = si_close(i, r);
	if (ssunlikely(rc == -1))
		return -1;
	/* remove directory */
	rc = si_dropof(scheme, r);
	return rc;
}
