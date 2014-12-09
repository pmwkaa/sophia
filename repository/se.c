
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libse.h>

int se_init(se *e)
{
	e->conf = NULL;
	sd_ssinit(&e->snapshot);
	sr_mutexinit(&e->lock);
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

#define SE_NONE 0
#define SE_SS   1
#define SE_SSI  2

static inline int
se_process(char *name)
{
	/* snapshot */
	/* snapshot.incomplete */
	char *token = name;
	if (strcmp(token, "snapshot") == 0)
		return SE_SS;
	else
	if (strcmp(token, "snapshot.incomplete") == 0)
		return SE_SSI;
	return -1;
}

/*
	snapshot recover states
	-----------------------
	(1) snapshot
		- read snapshot file, check crc
	(2) snapshot and snapshot.incomplete
		- remove incomplete file (validate?)
		- do (1)
	(3) snapshot.incomplete
		- rename to snapshot
		- do (1)
	(4) none
		- init empty snapshot buffer
*/

static int
se_recover_snapshot(se *e, sr *r)
{
	srpath path;
	sr_pathset(&path, "%s/snapshot", e->conf->path);
	uint64_t size = 0;
	int rc = sr_filesize(path.path, &size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' stat error: %s",
		         path.path, strerror(errno));
		return -1;
	}
	srbuf buf;
	rc = sr_bufensure(&buf, r->a, size);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	srfile file;
	sr_fileinit(&file, r->a);
	rc = sr_fileopen(&file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' open error: %s",
		         path.path, strerror(errno));
		goto error;
	}
	rc = sr_filepread(&file, 0, buf.s, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' read error: %s",
		         path.path, strerror(errno));
		goto error;
	}
	rc = sd_ssopen(&e->snapshot, r, &buf);
	if (srunlikely(rc == -1))
		goto error;
	return 0;
error:
	sr_buffree(&buf, r->a);
	return -1;
}

static int
se_recover(se *e, sr *r)
{
	DIR *dir = opendir(e->conf->path);
	if (srunlikely(dir == NULL)) {
		sr_error(r->e, "directory '%s' open error: %s",
		         e->conf->path, strerror(errno));
		return -1;
	}
	int snapshot_recover = SE_NONE;
	int rc;
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (srunlikely(de->d_name[0] == '.'))
			continue;
		int id = se_process(de->d_name);
		if (id == -1) /* skip unknown files */
			continue;
		snapshot_recover |= id;
	}
	closedir(dir);
	srpath path;
	srpath path_b;
	switch (snapshot_recover) {
	case SE_NONE: return 0;
	case SE_SS|SE_SSI:
		sr_pathset(&path, "%s/snapshot.incomplete", e->conf->path);
		rc = sr_fileunlink(path.path);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "snapshot file '%s' unlink error: %s",
			         path.path, strerror(errno));
			return -1;
		}
		break;
	case SE_SSI:
		sr_pathset(&path_b, "%s/snapshot.incomplete", e->conf->path);
		sr_pathset(&path, "%s/snapshot", e->conf->path);
		rc = sr_filemove(path_b.path, path.path);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "snapshot file '%s' rename error: %s",
			         path_b.path, strerror(errno));
			return -1;
		}
		break;
	case SE_SS:
		break;
	}
	return se_recover_snapshot(e, r);
}

int se_open(se *e, sr *r, seconf *conf)
{
	e->conf = conf;
	int exists = sr_fileexists(conf->path);
	if (exists == 0)
		return se_deploy(e, r);
	return se_recover(e, r);
}

int se_close(se *e, sr *r)
{
	sd_ssfree(&e->snapshot, r);
	sr_mutexfree(&e->lock);
	return 0;
}

static int
se_snapshot_update(se *e, sr *r, uint64_t lsn, char *name, int remove)
{
	se_lock(e);

	int rc = 0;
	if (remove) {
		rc = sd_ssdelete(&e->snapshot, r, name);
		(void)lsn;
	} else {
		rc = sd_ssadd(&e->snapshot, r, lsn, name);
	}
	if (srunlikely(rc == -1))
		goto error;

	srpath path;
	sr_pathset(&path, "%s/snapshot", e->conf->path);
	srpath path_b;
	sr_pathset(&path_b, "%s/snapshot.incomplete", e->conf->path);

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_0,
	             se_unlock(e);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	srfile file;
	sr_fileinit(&file, r->a);
	uint64_t size = 0;
	rc = sr_filenew(&file, path_b.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' create error: %s",
		         path.path, strerror(errno));
		return -1;
	}

	rc = sr_filewrite(&file, e->snapshot.buf.s, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' write error: %s",
		         file.file, strerror(errno));
		goto error;
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_1,
	             se_unlock(e);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	rc = sr_filesync(&file);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' sync error: %s",
		         file.file, strerror(errno));
		goto error;
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_2,
	             se_unlock(e);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	rc = sr_fileunlink(path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' unlink error: %s",
		         path.path, strerror(errno));
		return -1;
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_3,
	             se_unlock(e);
	             sr_error(r->e, "%s", "error injection");
	             return -1);

	rc = sr_filemove(path_b.path, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' rename error: %s",
		         path_b.path, strerror(errno));
		return -1;
	}

	se_unlock(e);
	return 0;

error:
	se_unlock(e);
	return -1;
}

int se_snapshot(se *e, sr *r, uint64_t lsn, char *name)
{
	return se_snapshot_update(e, r, lsn, name, 0);
}

int se_snapshot_remove(se *e, sr *r, char *name)
{
	return se_snapshot_update(e, r, 0 /* unused */, name, 1);
}
