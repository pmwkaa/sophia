
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
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_snapshot(si *index, siplan *plan)
{
	sr *r = &index->r;

	ssfile file;
	ss_fileinit(&file, r->vfs);

	/* prepare to take snapshot */
	sdsnapshot snapshot;
	sd_snapshot_init(&snapshot);
	int rc = ss_bufensure(&snapshot.buf, r->a, 1 * 1024 * 1024);
	if (ssunlikely(rc == -1))
		goto error_oom;
	rc = sd_snapshot_begin(&snapshot, r);
	if (ssunlikely(rc == -1))
		goto error_oom;

	/* save node index image */
	si_lock(index);
	ssrbnode *p = NULL;
	while ((p = ss_rbnext(&index->i, p)))
	{
		sinode *n = sscast(p, sinode, node);
		rc = sd_snapshot_add(&snapshot, r, n->self.id.id,
		                     n->file.size,
		                     n->branch_count,
		                     n->temperature_reads);
		if (ssunlikely(rc == -1)) {
			si_unlock(index);
			goto error_oom;
		}
		sibranch *b = &n->self;
		while (b) {
			rc = sd_snapshot_addbranch(&snapshot, r, b->index.h);
			if (ssunlikely(rc == -1)) {
				si_unlock(index);
				goto error_oom;
			}
			b = b->link;
		}
	}
	sd_snapshot_commit(&snapshot, r,
	                   index->lru_v,
	                   index->lru_steps,
	                   index->lru_intr_lsn,
	                   index->lru_intr_sum,
	                   index->read_disk,
	                   index->read_cache);
	si_unlock(index);

	/* create snapshot.inprogress */
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/index.incomplete",
	         index->scheme.path);
	rc = ss_filenew(&file, path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "index file '%s' create error: %s",
		               path, strerror(errno));
		goto error;
	}
	rc = ss_filewrite(&file, snapshot.buf.s, ss_bufused(&snapshot.buf));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "index file '%s' write error: %s",
		               path, strerror(errno));
		goto error;
	}

	SS_INJECTION(r->i, SS_INJECTION_SI_SNAPSHOT_0,
	             ss_fileclose(&file);
	             sd_snapshot_free(&snapshot, r);
				 sr_malfunction(r->e, "%s", "error injection");
				 return -1);

	/* sync snapshot file */
	if (index->scheme.sync) {
		rc = ss_filesync(&file);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "index file '%s' sync error: %s",
			               path, strerror(errno));
			goto error;
		}
	}

	/* remove old snapshot file (if exists) */
	snprintf(path, sizeof(path), "%s/index", index->scheme.path);
	ss_vfsunlink(r->vfs, path);

	SS_INJECTION(r->i, SS_INJECTION_SI_SNAPSHOT_1,
	             ss_fileclose(&file);
	             sd_snapshot_free(&snapshot, r);
				 sr_malfunction(r->e, "%s", "error injection");
				 return -1);

	/* rename snapshot.incomplete to snapshot */
	rc = ss_filerename(&file, path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "index file '%s' rename error: %s",
		               ss_pathof(&file.path),
		               strerror(errno));
		goto error;
	}

	SS_INJECTION(r->i, SS_INJECTION_SI_SNAPSHOT_2,
	             ss_fileclose(&file);
	             sd_snapshot_free(&snapshot, r);
				 sr_malfunction(r->e, "%s", "error injection");
				 return -1);

	/* close snapshot file */
	rc = ss_fileclose(&file);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "index file '%s' close error: %s",
		               path, strerror(errno));
		goto error;
	}

	sd_snapshot_free(&snapshot, r);

	/* finish index snapshot */
	si_lock(index);
	index->snapshot = plan->a;
	index->snapshot_run = 0;
	si_unlock(index);
	return 0;

error_oom:
	sr_oom(r->e);
error:
	ss_fileclose(&file);
	sd_snapshot_free(&snapshot, r);
	return -1;
}
