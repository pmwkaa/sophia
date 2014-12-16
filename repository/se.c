
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
	sr_spinlockinit(&e->lock);
	return 0;
}

static int
se_deploy_snapshot(se *e, sr *r)
{
	/* create empty snapshot */
	int rc;
	rc = sd_sscreate(&e->snapshot, r);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return -1;
	}
	srpath path;
	sr_pathset(&path, "%s/snapshot", e->conf->path);
	srfile file;
	sr_fileinit(&file, r->a);
	rc = sr_filenew(&file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' create error: %s",
		         path.path, strerror(errno));
		return -1;
	}
	uint64_t size = sr_bufused(&e->snapshot.buf);
	rc = sr_filewrite(&file, e->snapshot.buf.s, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' write error: %s",
		         file.file, strerror(errno));
		goto error;
	}
	if (e->conf->sync) {
		rc = sr_filesync(&file);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "snapshot file '%s' sync error: %s",
					 file.file, strerror(errno));
			goto error;
		}
	}
	rc = sr_fileclose(&file);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' close error: %s",
		         file.file, strerror(errno));
		return -1;
	}
	return 0;
error:
	sr_fileclose(&file);
	return -1;
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
	return se_deploy_snapshot(e, r);
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
	sr_bufinit(&buf);
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
	sr_bufadvance(&buf, size);
	rc = sd_ssopen(&e->snapshot, r, &buf);
	if (srunlikely(rc == -1))
		goto error;
	sr_fileclose(&file);
	return 0;
error:
	sr_fileclose(&file);
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
	case SE_NONE:
		/* create empty snapshot, if no snapshot file is found */
		return se_deploy_snapshot(e, r);
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
	sr_spinlockfree(&e->lock);
	return 0;
}

static int
se_snapshot_update(se *e, sr *r, uint64_t lsn, char *name, int remove)
{
	se_lock(e);
	sdss n;
	int has_snapshot = sr_bufused(&e->snapshot.buf);
	int rc = 0;
	if (remove) {
		rc = sd_ssdelete(&e->snapshot, &n, r, name);
		(void)lsn;
	} else {
		rc = sd_ssadd(&e->snapshot, &n, r, lsn, name);
	}
	se_unlock(e);
	if (srunlikely(rc == -1))
		return -1;

	srpath path;
	sr_pathset(&path, "%s/snapshot", e->conf->path);
	srpath path_b;
	sr_pathset(&path_b, "%s/snapshot.incomplete", e->conf->path);

	srfile file;
	sr_fileinit(&file, r->a);
	rc = sr_filenew(&file, path_b.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' create error: %s",
		         path.path, strerror(errno));
		goto e0;
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_0,
	             sr_error(r->e, "%s", "error injection");
	             goto e0);

	uint64_t size = sr_bufused(&n.buf);
	rc = sr_filewrite(&file, n.buf.s, size);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' write error: %s",
		         file.file, strerror(errno));
		goto e1;
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_1,
	             sr_error(r->e, "%s", "error injection");
	             goto e1);

	if (e->conf->sync) {
		rc = sr_filesync(&file);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "snapshot file '%s' sync error: %s",
					 file.file, strerror(errno));
			goto e1;
		}
	}

	rc = sr_fileclose(&file);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' close error: %s",
		         file.file, strerror(errno));
		goto e1;
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_2,
	             sr_error(r->e, "%s", "error injection");
	             goto e1);

	if (has_snapshot > 0) {
		rc = sr_fileunlink(path.path);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "snapshot file '%s' unlink error: %s",
					 path.path, strerror(errno));
			goto e0;
		}
	}

	SR_INJECTION(r->i, SR_INJECTION_SE_SNAPSHOT_3,
	             sr_error(r->e, "%s", "error injection");
	             goto e1);

	rc = sr_filemove(path_b.path, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "snapshot file '%s' rename error: %s",
		         path_b.path, strerror(errno));
		goto e0;
	}

	se_lock(e);
	sdss prev = e->snapshot;
	e->snapshot = n;
	se_unlock(e);

	sd_ssfree(&prev, r);
	return 0;
e1:
	sr_fileclose(&file);
e0:
	sd_ssfree(&n, r);
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
