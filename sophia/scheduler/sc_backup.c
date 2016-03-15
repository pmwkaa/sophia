
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
#include <libsl.h>
#include <libsi.h>
#include <libsy.h>
#include <libsc.h>

int sc_backupstart(sc *s)
{
	if (ssunlikely(s->backup_path == NULL)) {
		sr_error(s->r->e, "%s", "backup is not enabled");
		return -1;
	}
	/* begin backup procedure
	 * state 0
	 *
	 * disable log garbage-collection
	*/
	sl_poolgc_enable(s->lp, 0);
	ss_mutexlock(&s->lock);
	if (ssunlikely(s->backup > 0)) {
		ss_mutexunlock(&s->lock);
		sl_poolgc_enable(s->lp, 1);
		/* in progress */
		return 0;
	}
	uint64_t bsn = sr_seq(s->r->seq, SR_BSNNEXT);
	s->backup = 1;
	s->backup_bsn = bsn;
	sc_start(s, SI_BACKUP);
	ss_mutexunlock(&s->lock);
	return 0;
}

int sc_backupbegin(sc *s)
{
	/*
	 * a. create backup_path/<bsn.incomplete> directory
	 * b. create database directories
	 * c. create log directory
	*/
	char path[1024];
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         s->backup_path, s->backup_bsn);
	int rc = ss_vfsmkdir(s->r->vfs, path, 0755);
	if (ssunlikely(rc == -1)) {
		sr_error(s->r->e, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	int i = 0;
	while (i < s->count) {
		scdb *db = s->i[i];
		snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/%s",
		         s->backup_path, s->backup_bsn,
		         db->index->scheme.name);
		rc = ss_vfsmkdir(s->r->vfs, path, 0755);
		if (ssunlikely(rc == -1)) {
			sr_error(s->r->e, "backup directory '%s' create error: %s",
			         path, strerror(errno));
			return -1;
		}
		i++;
	}
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/log",
	         s->backup_path, s->backup_bsn);
	rc = ss_vfsmkdir(s->r->vfs, path, 0755);
	if (ssunlikely(rc == -1)) {
		sr_error(s->r->e, "backup directory '%s' create error: %s",
		         path, strerror(errno));
		return -1;
	}
	return 0;
}

int sc_backupend(sc *s, scworker *w)
{
	/*
	 * a. rotate log file
	 * b. copy log files
	 * c. enable log gc
	 * d. rename <bsn.incomplete> into <bsn>
	 * e. set last backup, set COMPLETE
	 */

	/* force log rotation */
	ss_trace(&w->trace, "%s", "log rotation for backup");
	int rc = sl_poolrotate(s->lp);
	if (ssunlikely(rc == -1))
		return -1;

	/* copy log files */
	ss_trace(&w->trace, "%s", "log files backup");

	char path[1024];
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete/log",
	         s->backup_path, s->backup_bsn);
	rc = sl_poolcopy(s->lp, path, &w->dc.c);
	if (ssunlikely(rc == -1)) {
		sr_errorrecover(s->r->e);
		return -1;
	}

	/* enable log gc */
	sl_poolgc_enable(s->lp, 1);

	/* complete backup */
	snprintf(path, sizeof(path), "%s/%" PRIu32 ".incomplete",
	         s->backup_path, s->backup_bsn);
	char newpath[1024];
	snprintf(newpath, sizeof(newpath), "%s/%" PRIu32,
	         s->backup_path, s->backup_bsn);
	rc = ss_vfsrename(s->r->vfs, path, newpath);
	if (ssunlikely(rc == -1)) {
		sr_error(s->r->e, "backup directory '%s' rename error: %s",
		         path, strerror(errno));
		return -1;
	}

	/* complete */
	s->backup_bsn_last = s->backup_bsn;
	s->backup_bsn_last_complete = 1;
	s->backup = 0;
	s->backup_bsn = 0;
	return 0;
}

int sc_backuperror(sc *s)
{
	sl_poolgc_enable(s->lp, 1);
	s->backup = 0;
	s->backup_bsn_last_complete = 0;
	return 0;
}
