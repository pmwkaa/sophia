
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
#include <libsw.h>

static inline sw*
sw_alloc(swmanager *p, uint64_t id)
{
	sw *l = ss_malloc(p->r->a, sizeof(*l));
	if (ssunlikely(l == NULL)) {
		sr_oom_malfunction(p->r->e);
		return NULL;
	}
	l->id = id;
	l->p  = NULL;
	ss_gcinit(&l->gc);
	ss_mutexinit(&l->filelock);
	ss_fileinit(&l->file, p->r->vfs);
	ss_listinit(&l->link);
	ss_listinit(&l->linkcopy);
	return l;
}

static inline int
sw_close(swmanager *p, sw *l)
{
	int rc = ss_fileclose(&l->file);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' close error: %s",
		               ss_pathof(&l->file.path),
		               strerror(errno));
	}
	ss_mutexfree(&l->filelock);
	ss_gcfree(&l->gc);
	ss_free(p->r->a, l);
	return rc;
}

static inline sw*
sw_open(swmanager *p, uint64_t id)
{
	sw *l = sw_alloc(p, id);
	if (ssunlikely(l == NULL))
		return NULL;
	sspath path;
	ss_path(&path, p->conf.path, id, ".log");
	int rc = ss_fileopen(&l->file, path.path, 0);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' open error: %s",
		               ss_pathof(&l->file.path),
		               strerror(errno));
		goto error;
	}
	return l;
error:
	sw_close(p, l);
	return NULL;
}

static inline sw*
sw_new(swmanager *p, uint64_t id)
{
	sw *l = sw_alloc(p, id);
	if (ssunlikely(l == NULL))
		return NULL;
	sspath path;
	ss_path(&path, p->conf.path, id, ".log");
	int rc = ss_filenew(&l->file, path.path, 0);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' create error: %s",
		               path.path, strerror(errno));
		goto error;
	}
	srversion v;
	sr_version_storage(&v);
	rc = ss_filewrite(&l->file, &v, sizeof(v));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' header write error: %s",
		               ss_pathof(&l->file.path),
		               strerror(errno));
		goto error;
	}
	return l;
error:
	sw_close(p, l);
	return NULL;
}

int sw_managerinit(swmanager *p, sr *r)
{
	ss_spinlockinit(&p->lock);
	ss_listinit(&p->list);
	sw_confinit(&p->conf);
	p->n    = 0;
	p->r    = r;
	p->gc   = 1;
	struct iovec *iov =
		ss_malloc(r->a, sizeof(struct iovec) * 1021);
	if (ssunlikely(iov == NULL))
		return sr_oom_malfunction(r->e);
	ss_iovinit(&p->iov, iov, 1021);
	return 0;
}

static inline int
sw_managercreate(swmanager *p)
{
	int rc;
	rc = ss_vfsmkdir(p->r->vfs, p->conf.path, 0755);
	if (ssunlikely(rc == -1))
		return sr_malfunction(p->r->e, "log directory '%s' create error: %s",
		                      p->conf.path, strerror(errno));
	return 1;
}

static inline int
sw_managerrecover(swmanager *p)
{
	ssbuf list;
	ss_bufinit(&list);
	swdirtype types[] =
	{
		{ "log", 1, 0 },
		{ NULL,  0, 0 }
	};
	int rc = sw_dirread(&list, p->r->a, types, p->conf.path);
	if (ssunlikely(rc == -1))
		return sr_malfunction(p->r->e, "log directory '%s' open error",
		                      p->conf.path);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &list, sizeof(swdirid));
	while(ss_iterhas(ss_bufiter, &i)) {
		swdirid *id = ss_iterof(ss_bufiter, &i);
		sw *l = sw_open(p, id->id);
		if (ssunlikely(l == NULL)) {
			ss_buffree(&list, p->r->a);
			return -1;
		}
		l->gc.complete = 1;
		ss_listappend(&p->list, &l->link);
		p->n++;
		ss_iternext(ss_bufiter, &i);
	}
	ss_buffree(&list, p->r->a);
	if (p->n) {
		sw *last = sscast(p->list.prev, sw, link);
		last->gc.complete = 0;
		rc = ss_fileseek(&last->file, last->file.size);
		if (ssunlikely(rc == -1)) {
			return sr_malfunction(p->r->e, "log file '%s' seek error: %s",
			                      ss_pathof(&last->file.path),
			                      strerror(errno));
		}
		p->r->seq->lfsn = last->id;
		p->r->seq->lfsn++;
	}
	return 0;
}

int sw_manageropen(swmanager *p)
{
	if (ssunlikely(! p->conf.enable))
		return 0;
	int exists = ss_vfsexists(p->r->vfs, p->conf.path);
	int rc;
	if (! exists) {
		rc = sw_managercreate(p);
		if (ssunlikely(rc == -1))
			return -1;
		rc = sw_managerrotate(p);
		if (ssunlikely(rc == -1))
			return -1;
	} else {
		rc = sw_managerrecover(p);
		if (ssunlikely(rc == -1))
			return -1;
	}
	return 0;
}

int sw_managerrotate(swmanager *p)
{
	if (ssunlikely(! p->conf.enable))
		return 0;
	uint64_t lfsn = sr_seq(p->r->seq, SR_LFSNNEXT);
	sw *l = sw_new(p, lfsn);
	if (ssunlikely(l == NULL))
		return -1;
	sw *log = NULL;
	ss_spinlock(&p->lock);
	if (p->n)
		log = sscast(p->list.prev, sw, link);
	ss_listappend(&p->list, &l->link);
	p->n++;
	ss_spinunlock(&p->lock);
	if (log) {
		assert(log->file.fd != -1);
		if (p->conf.sync_on_rotate) {
			int rc = ss_filesync(&log->file);
			if (ssunlikely(rc == -1)) {
				sr_malfunction(p->r->e, "log file '%s' sync error: %s",
				               ss_pathof(&log->file.path),
				               strerror(errno));
				return -1;
			}
		}
		ss_fileadvise(&log->file, 0, 0, log->file.size);
		ss_gccomplete(&log->gc);
	}
	return 0;
}

int sw_managerrotate_ready(swmanager *p)
{
	if (ssunlikely(! p->conf.enable))
		return 0;
	ss_spinlock(&p->lock);
	assert(p->n > 0);
	sw *l = sscast(p->list.prev, sw, link);
	int ready = ss_gcrotateready(&l->gc, p->conf.rotatewm);
	ss_spinunlock(&p->lock);
	return ready;
}

int sw_managershutdown(swmanager *p)
{
	int rcret = 0;
	int rc;
	if (p->n) {
		sslist *i, *n;
		ss_listforeach_safe(&p->list, i, n) {
			sw *l = sscast(i, sw, link);
			rc = sw_close(p, l);
			if (ssunlikely(rc == -1))
				rcret = -1;
		}
	}
	if (p->iov.v)
		ss_free(p->r->a, p->iov.v);
	sw_conffree(&p->conf, p->r->a);
	ss_spinlockfree(&p->lock);
	return rcret;
}

static inline int
sw_gc(swmanager *p, sw *l)
{
	int rc;
	rc = ss_vfsunlink(p->r->vfs, ss_pathof(&l->file.path));
	if (ssunlikely(rc == -1)) {
		return sr_malfunction(p->r->e, "log file '%s' unlink error: %s",
		                      ss_pathof(&l->file.path),
		                      strerror(errno));
	}
	rc = sw_close(p, l);
	if (ssunlikely(rc == -1))
		return -1;
	return 1;
}

int sw_managergc_enable(swmanager *p, int enable)
{
	ss_spinlock(&p->lock);
	p->gc = enable;
	ss_spinunlock(&p->lock);
	return 0;
}

int sw_managergc(swmanager *p)
{
	if (ssunlikely(! p->conf.enable))
		return 0;
	for (;;) {
		ss_spinlock(&p->lock);
		if (ssunlikely(! p->gc)) {
			ss_spinunlock(&p->lock);
			return 0;
		}
		sw *current = NULL;
		sslist *i;
		ss_listforeach(&p->list, i) {
			sw *l = sscast(i, sw, link);
			if (sslikely(! ss_gcgarbage(&l->gc)))
				continue;
			ss_listunlink(&l->link);
			p->n--;
			current = l;
			break;
		}
		ss_spinunlock(&p->lock);
		if (current) {
			int rc = sw_gc(p, current);
			if (ssunlikely(rc == -1))
				return -1;
		} else {
			break;
		}
	}
	return 0;
}

int sw_managerfiles(swmanager *p)
{
	ss_spinlock(&p->lock);
	int n = p->n;
	ss_spinunlock(&p->lock);
	return n;
}

int sw_managercopy(swmanager *p, char *dest, ssbuf *buf)
{
	sslist list;
	ss_listinit(&list);
	ss_spinlock(&p->lock);
	sslist *i;
	ss_listforeach(&p->list, i) {
		sw *l = sscast(i, sw, link);
		if (ss_gcinprogress(&l->gc))
			break;
		ss_listappend(&list, &l->linkcopy);
	}
	ss_spinunlock(&p->lock);

	ss_bufreset(buf);
	sslist *n;
	ss_listforeach_safe(&list, i, n)
	{
		sw *l = sscast(i, sw, linkcopy);
		ss_listinit(&l->linkcopy);
		sspath path;
		ss_path(&path, dest, l->id, ".log");
		ssfile file;
		ss_fileinit(&file, p->r->vfs);
		int rc = ss_filenew(&file, path.path, 0);
		if (ssunlikely(rc == -1)) {
			sr_error(p->r->e, "log file '%s' create error: %s",
			         path.path, strerror(errno));
			return -1;
		}
		rc = ss_bufensure(buf, p->r->a, l->file.size);
		if (ssunlikely(rc == -1)) {
			sr_oom(p->r->e);
			ss_fileclose(&file);
			return -1;
		}
		rc = ss_filepread(&l->file, 0, buf->s, l->file.size);
		if (ssunlikely(rc == -1)) {
			sr_error(p->r->e, "log file '%s' read error: %s",
			         ss_pathof(&l->file.path),
			         strerror(errno));
			ss_fileclose(&file);
			return -1;
		}
		ss_bufadvance(buf, l->file.size);
		rc = ss_filewrite(&file, buf->s, l->file.size);
		if (ssunlikely(rc == -1)) {
			sr_error(p->r->e, "log file '%s' write error: %s",
			         path.path,
			         strerror(errno));
			ss_fileclose(&file);
			return -1;
		}
		/* sync? */
		rc = ss_fileclose(&file);
		if (ssunlikely(rc == -1)) {
			sr_error(p->r->e, "log file '%s' close error: %s",
			         path.path, strerror(errno));
			return -1;
		}
		ss_bufreset(buf);
	}
	return 0;
}

int sw_begin(swmanager *p, swtx *t, uint64_t lsn, int recover)
{
	ss_spinlock(&p->lock);
	if (sslikely(lsn == 0)) {
		lsn = sr_seq(p->r->seq, SR_LSNNEXT);
	} else {
		sr_seqlock(p->r->seq);
		if (lsn > p->r->seq->lsn)
			p->r->seq->lsn = lsn;
		sr_sequnlock(p->r->seq);
	}
	t->lsn = lsn;
	t->recover = recover;
	t->svp = 0;
	t->p = p;
	t->l = NULL;
	if (! p->conf.enable)
		return 0;
	assert(p->n > 0);
	sw *l = sscast(p->list.prev, sw, link);
	ss_mutexlock(&l->filelock);
	t->svp = ss_filesvp(&l->file);
	t->l = l;
	t->p = p;
	return 0;
}

int sw_commit(swtx *t)
{
	if (t->p->conf.enable)
		ss_mutexunlock(&t->l->filelock);
	ss_spinunlock(&t->p->lock);
	return 0;
}

int sw_rollback(swtx *t)
{
	int rc = 0;
	if (t->p->conf.enable) {
		rc = ss_filerlb(&t->l->file, t->svp);
		if (ssunlikely(rc == -1))
			sr_malfunction(t->p->r->e, "log file '%s' truncate error: %s",
			               ss_pathof(&t->l->file.path),
			               strerror(errno));
		ss_mutexunlock(&t->l->filelock);
	}
	ss_spinunlock(&t->p->lock);
	return rc;
}

static inline void
sw_writeadd(swmanager *p, swtx *t, svlog *vlog, swv *lv, svlogv *logv)
{
	sr *r = sv_logindex(vlog, logv->index_id)->r;
	char *data = sv_vpointer(logv->v);
	lv->dsn   = logv->index_id;
	lv->flags = sf_flags(r->scheme, data);
	lv->size  = sf_size(r->scheme, data);
	lv->crc   = ss_crcp(p->r->crc, data, lv->size, 0);
	lv->crc   = ss_crcs(p->r->crc, lv, sizeof(swv), lv->crc);
	ss_iovadd(&p->iov, lv, sizeof(swv));
	ss_iovadd(&p->iov, data, lv->size);
	logv->v->log = t->l;
}

static inline int
sw_writestmt(swtx *t, svlog *vlog)
{
	swmanager *p = t->p;
	svlogv *stmt = NULL;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &vlog->buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i)) {
		svlogv *logv = ss_iterof(ss_bufiter, &i);
		svv *v = logv->v;
		sr *r = sv_logindex(vlog, logv->index_id)->r;
		sf_lsnset(r->scheme, sv_vpointer(v), t->lsn);
		if (sslikely(! (sf_is(r->scheme, sv_vpointer(v), SVGET)))) {
			assert(stmt == NULL);
			stmt = logv;
		}
	}
	assert(stmt != NULL);
	swv lv;
	sw_writeadd(t->p, t, vlog, &lv, stmt);
	int rc = ss_filewritev(&t->l->file, &p->iov);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' write error: %s",
		               ss_pathof(&t->l->file.path),
		               strerror(errno));
		return -1;
	}
	ss_gcmark(&t->l->gc, 1);
	ss_iovreset(&p->iov);
	return 0;
}

static int
sw_writestmt_multi(swtx *t, svlog *vlog)
{
	swmanager *p = t->p;
	sw *l = t->l;
	swv lvbuf[510]; /* 1 + 510 per syscall */
	int lvp;
	int rc;
	lvp = 0;
	/* transaction header */
	swv *lv = &lvbuf[0];
	lv->dsn   = 0;
	lv->flags = SVBEGIN;
	lv->size  = sv_logcount_write(vlog);
	lv->crc   = ss_crcs(p->r->crc, lv, sizeof(swv), 0);
	ss_iovadd(&p->iov, lv, sizeof(swv));
	lvp++;
	/* body */
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &vlog->buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		if (ssunlikely(! ss_iovensure(&p->iov, 2))) {
			rc = ss_filewritev(&l->file, &p->iov);
			if (ssunlikely(rc == -1)) {
				sr_malfunction(p->r->e, "log file '%s' write error: %s",
				               ss_pathof(&l->file.path),
				               strerror(errno));
				return -1;
			}
			ss_iovreset(&p->iov);
			lvp = 0;
		}
		svlogv *logv = ss_iterof(ss_bufiter, &i);
		svv *v = logv->v;
		sr *r = sv_logindex(vlog, logv->index_id)->r;
		sf_lsnset(r->scheme, sv_vpointer(v), t->lsn);
		if (sf_is(r->scheme, sv_vpointer(v), SVGET))
			continue;
		lv = &lvbuf[lvp];
		sw_writeadd(p, t, vlog, lv, logv);
		lvp++;
	}
	if (sslikely(ss_iovhas(&p->iov))) {
		rc = ss_filewritev(&l->file, &p->iov);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(p->r->e, "log file '%s' write error: %s",
			               ss_pathof(&l->file.path),
			               strerror(errno));
			return -1;
		}
		ss_iovreset(&p->iov);
	}
	ss_gcmark(&l->gc, sv_logcount_write(vlog));
	return 0;
}

int sw_write(swtx *t, svlog *vlog)
{
	int count = sv_logcount_write(vlog);
	/* fast path for log-disabled, recover or
	 * ro-transactions
	 */
	if (t->recover || !t->p->conf.enable || count == 0)
	{
		ssiter i;
		ss_iterinit(ss_bufiter, &i);
		ss_iteropen(ss_bufiter, &i, &vlog->buf, sizeof(svlogv));
		for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
		{
			svlogv *v = ss_iterof(ss_bufiter, &i);
			sr *r = sv_logindex(vlog, v->index_id)->r;
			sf_lsnset(r->scheme, sv_vpointer(v->v), t->lsn);
		}
		return 0;
	}

	/* write single or multi-stmt transaction */
	int rc;
	if (sslikely(count == 1)) {
		rc = sw_writestmt(t, vlog);
	} else {
		rc = sw_writestmt_multi(t, vlog);
	}
	if (ssunlikely(rc == -1))
		return -1;

	/* sync */
	if (t->p->conf.sync_on_write) {
		rc = ss_filesync(&t->l->file);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(t->p->r->e, "log file '%s' sync error: %s",
			               ss_pathof(&t->l->file.path),
			               strerror(errno));
			return -1;
		}
	}
	return 0;
}
