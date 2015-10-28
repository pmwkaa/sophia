
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
#include <libsl.h>

static inline sl*
sl_alloc(slpool *p, uint32_t id)
{
	sl *l = ss_malloc(p->r->a, sizeof(*l));
	if (ssunlikely(l == NULL)) {
		sr_oom_malfunction(p->r->e);
		return NULL;
	}
	l->id   = id;
	l->p    = NULL;
	ss_gcinit(&l->gc);
	ss_mutexinit(&l->filelock);
	ss_fileinit(&l->file, p->r->vfs);
	ss_listinit(&l->link);
	ss_listinit(&l->linkcopy);
	return l;
}

static inline int
sl_close(slpool *p, sl *l)
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

static inline sl*
sl_open(slpool *p, uint32_t id)
{
	sl *l = sl_alloc(p, id);
	if (ssunlikely(l == NULL))
		return NULL;
	sspath path;
	ss_pathA(&path, p->conf->path, id, ".log");
	int rc = ss_fileopen(&l->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' open error: %s",
		               ss_pathof(&l->file.path),
		               strerror(errno));
		goto error;
	}
	return l;
error:
	sl_close(p, l);
	return NULL;
}

static inline sl*
sl_new(slpool *p, uint32_t id)
{
	sl *l = sl_alloc(p, id);
	if (ssunlikely(l == NULL))
		return NULL;
	sspath path;
	ss_pathA(&path, p->conf->path, id, ".log");
	int rc = ss_filenew(&l->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' create error: %s",
		               path.path, strerror(errno));
		goto error;
	}
	srversion v;
	sr_version(&v);
	rc = ss_filewrite(&l->file, &v, sizeof(v));
	if (ssunlikely(rc == -1)) {
		sr_malfunction(p->r->e, "log file '%s' header write error: %s",
		               ss_pathof(&l->file.path),
		               strerror(errno));
		goto error;
	}
	return l;
error:
	sl_close(p, l);
	return NULL;
}

int sl_poolinit(slpool *p, sr *r)
{
	ss_spinlockinit(&p->lock);
	ss_listinit(&p->list);
	p->n    = 0;
	p->r    = r;
	p->gc   = 1;
	p->conf = NULL;
	struct iovec *iov =
		ss_malloc(r->a, sizeof(struct iovec) * 1021);
	if (ssunlikely(iov == NULL))
		return sr_oom_malfunction(r->e);
	ss_iovinit(&p->iov, iov, 1021);
	return 0;
}

static inline int
sl_poolcreate(slpool *p)
{
	int rc;
	rc = ss_vfsmkdir(p->r->vfs, p->conf->path, 0755);
	if (ssunlikely(rc == -1))
		return sr_malfunction(p->r->e, "log directory '%s' create error: %s",
		                      p->conf->path, strerror(errno));
	return 1;
}

static inline int
sl_poolrecover(slpool *p)
{
	ssbuf list;
	ss_bufinit(&list);
	sldirtype types[] =
	{
		{ "log", 1, 0 },
		{ NULL,  0, 0 }
	};
	int rc = sl_dirread(&list, p->r->a, types, p->conf->path);
	if (ssunlikely(rc == -1))
		return sr_malfunction(p->r->e, "log directory '%s' open error",
		                      p->conf->path);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &list, sizeof(sldirid));
	while(ss_iterhas(ss_bufiter, &i)) {
		sldirid *id = ss_iterof(ss_bufiter, &i);
		sl *l = sl_open(p, id->id);
		if (ssunlikely(l == NULL)) {
			ss_buffree(&list, p->r->a);
			return -1;
		}
		ss_listappend(&p->list, &l->link);
		p->n++;
		ss_iternext(ss_bufiter, &i);
	}
	ss_buffree(&list, p->r->a);
	if (p->n) {
		sl *last = sscast(p->list.prev, sl, link);
		p->r->seq->lfsn = last->id;
		p->r->seq->lfsn++;
	}
	return 0;
}

int sl_poolopen(slpool *p, slconf *conf)
{
	p->conf = conf;
	if (ssunlikely(! p->conf->enable))
		return 0;
	int exists = ss_vfsexists(p->r->vfs, p->conf->path);
	int rc;
	if (! exists)
		rc = sl_poolcreate(p);
	else
		rc = sl_poolrecover(p);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

int sl_poolrotate(slpool *p)
{
	if (ssunlikely(! p->conf->enable))
		return 0;
	uint32_t lfsn = sr_seq(p->r->seq, SR_LFSNNEXT);
	sl *l = sl_new(p, lfsn);
	if (ssunlikely(l == NULL))
		return -1;
	sl *log = NULL;
	ss_spinlock(&p->lock);
	if (p->n) {
		log = sscast(p->list.prev, sl, link);
		ss_gccomplete(&log->gc);
	}
	ss_listappend(&p->list, &l->link);
	p->n++;
	ss_spinunlock(&p->lock);
	if (log) {
		if (p->conf->sync_on_rotate) {
			int rc = ss_filesync(&log->file);
			if (ssunlikely(rc == -1)) {
				sr_malfunction(p->r->e, "log file '%s' sync error: %s",
				               ss_pathof(&log->file.path),
				               strerror(errno));
				return -1;
			}
		}
	}
	return 0;
}

int sl_poolrotate_ready(slpool *p, int wm)
{
	if (ssunlikely(! p->conf->enable))
		return 0;
	ss_spinlock(&p->lock);
	assert(p->n > 0);
	sl *l = sscast(p->list.prev, sl, link);
	int ready = ss_gcrotateready(&l->gc, wm);
	ss_spinunlock(&p->lock);
	return ready;
}

int sl_poolshutdown(slpool *p)
{
	int rcret = 0;
	int rc;
	if (p->n) {
		sslist *i, *n;
		ss_listforeach_safe(&p->list, i, n) {
			sl *l = sscast(i, sl, link);
			rc = sl_close(p, l);
			if (ssunlikely(rc == -1))
				rcret = -1;
		}
	}
	if (p->iov.v)
		ss_free(p->r->a, p->iov.v);
	ss_spinlockfree(&p->lock);
	return rcret;
}

static inline int
sl_gc(slpool *p, sl *l)
{
	int rc;
	rc = ss_vfsunlink(p->r->vfs, ss_pathof(&l->file.path));
	if (ssunlikely(rc == -1)) {
		return sr_malfunction(p->r->e, "log file '%s' unlink error: %s",
		                      ss_pathof(&l->file.path),
		                      strerror(errno));
	}
	rc = sl_close(p, l);
	if (ssunlikely(rc == -1))
		return -1;
	return 1;
}

int sl_poolgc_enable(slpool *p, int enable)
{
	ss_spinlock(&p->lock);
	p->gc = enable;
	ss_spinunlock(&p->lock);
	return 0;
}

int sl_poolgc(slpool *p)
{
	if (ssunlikely(! p->conf->enable))
		return 0;
	for (;;) {
		ss_spinlock(&p->lock);
		if (ssunlikely(! p->gc)) {
			ss_spinunlock(&p->lock);
			return 0;
		}
		sl *current = NULL;
		sslist *i;
		ss_listforeach(&p->list, i) {
			sl *l = sscast(i, sl, link);
			if (sslikely(! ss_gcgarbage(&l->gc)))
				continue;
			ss_listunlink(&l->link);
			p->n--;
			current = l;
			break;
		}
		ss_spinunlock(&p->lock);
		if (current) {
			int rc = sl_gc(p, current);
			if (ssunlikely(rc == -1))
				return -1;
		} else {
			break;
		}
	}
	return 0;
}

int sl_poolfiles(slpool *p)
{
	ss_spinlock(&p->lock);
	int n = p->n;
	ss_spinunlock(&p->lock);
	return n;
}

int sl_poolcopy(slpool *p, char *dest, ssbuf *buf)
{
	sslist list;
	ss_listinit(&list);
	ss_spinlock(&p->lock);
	sslist *i;
	ss_listforeach(&p->list, i) {
		sl *l = sscast(i, sl, link);
		if (ss_gcinprogress(&l->gc))
			break;
		ss_listappend(&list, &l->linkcopy);
	}
	ss_spinunlock(&p->lock);

	ss_bufinit(buf);
	sslist *n;
	ss_listforeach_safe(&list, i, n)
	{
		sl *l = sscast(i, sl, linkcopy);
		ss_listinit(&l->linkcopy);
		sspath path;
		ss_pathA(&path, dest, l->id, ".log");
		ssfile file;
		ss_fileinit(&file, p->r->vfs);
		int rc = ss_filenew(&file, path.path);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(p->r->e, "log file '%s' create error: %s",
			               path.path, strerror(errno));
			return -1;
		}
		rc = ss_bufensure(buf, p->r->a, l->file.size);
		if (ssunlikely(rc == -1)) {
			sr_oom_malfunction(p->r->e);
			ss_fileclose(&file);
			return -1;
		}
		rc = ss_filepread(&l->file, 0, buf->s, l->file.size);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(p->r->e, "log file '%s' read error: %s",
			               ss_pathof(&l->file.path),
			               strerror(errno));
			ss_fileclose(&file);
			return -1;
		}
		ss_bufadvance(buf, l->file.size);
		rc = ss_filewrite(&file, buf->s, l->file.size);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(p->r->e, "log file '%s' write error: %s",
			               path.path,
			               strerror(errno));
			ss_fileclose(&file);
			return -1;
		}
		/* sync? */
		rc = ss_fileclose(&file);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(p->r->e, "log file '%s' close error: %s",
			               path.path, strerror(errno));
			return -1;
		}
		ss_bufreset(buf);
	}
	return 0;
}

int sl_begin(slpool *p, sltx *t)
{
	memset(t, 0, sizeof(*t));
	ss_spinlock(&p->lock);
	t->p = p;
	if (! p->conf->enable)
		return 0;
	assert(p->n > 0);
	sl *l = sscast(p->list.prev, sl, link);
	ss_mutexlock(&l->filelock);
	t->svp = ss_filesvp(&l->file);
	t->l = l;
	t->p = p;
	return 0;
}

int sl_commit(sltx *t)
{
	if (t->p->conf->enable)
		ss_mutexunlock(&t->l->filelock);
	ss_spinunlock(&t->p->lock);
	return 0;
}

int sl_rollback(sltx *t)
{
	int rc = 0;
	if (t->p->conf->enable) {
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

static inline int
sl_follow(slpool *p, uint64_t lsn)
{
	sr_seqlock(p->r->seq);
	if (lsn > p->r->seq->lsn)
		p->r->seq->lsn = lsn;
	sr_sequnlock(p->r->seq);
	return 0;
}

int sl_prepare(slpool *p, svlog *vlog, uint64_t lsn)
{
	if (ssunlikely(sv_logcount_write(vlog) == 0))
		return 0;
	if (sslikely(lsn == 0))
		lsn = sr_seq(p->r->seq, SR_LSNNEXT);
	else
		sl_follow(p, lsn);
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &vlog->buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i))
	{
		svlogv *v = ss_iterof(ss_bufiter, &i);
		sv_lsnset(&v->v, lsn);
	}
	return 0;
}

static inline void
sl_write_prepare(slpool *p, sltx *t, slv *lv, svlogv *logv)
{
	sv *v = &logv->v;
	lv->lsn   = sv_lsn(v);
	lv->dsn   = logv->id;
	lv->flags = sv_flags(v);
	lv->size  = sv_size(v);
	lv->crc   = ss_crcp(p->r->crc, sv_pointer(v), lv->size, 0);
	lv->crc   = ss_crcs(p->r->crc, lv, sizeof(slv), lv->crc);
	ss_iovadd(&p->iov, lv, sizeof(slv));
	ss_iovadd(&p->iov, sv_pointer(v), lv->size);
	((svv*)v->v)->log = t->l;
}

static inline int
sl_write_stmt(sltx *t, svlog *vlog)
{
	slpool *p = t->p;
	svlogv *logv;
	ssiter i;
	ss_iterinit(ss_bufiter, &i);
	ss_iteropen(ss_bufiter, &i, &vlog->buf, sizeof(svlogv));
	for (; ss_iterhas(ss_bufiter, &i); ss_iternext(ss_bufiter, &i)) {
		logv = ss_iterof(ss_bufiter, &i);
		sv *v = &logv->v;
		if (! (sv_flags(v) & SVGET))
			break;
	}
	logv = ss_iterof(ss_bufiter, &i);
	assert(logv != NULL);
	slv lv;
	sl_write_prepare(t->p, t, &lv, logv);
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
sl_write_multi_stmt(sltx *t, svlog *vlog, uint64_t lsn)
{
	slpool *p = t->p;
	sl *l = t->l;
	slv lvbuf[510]; /* 1 + 510 per syscall */
	int lvp;
	int rc;
	lvp = 0;
	/* transaction header */
	slv *lv = &lvbuf[0];
	lv->lsn   = lsn;
	lv->dsn   = 0;
	lv->flags = SVBEGIN;
	lv->size  = sv_logcount_write(vlog);
	lv->crc   = ss_crcs(p->r->crc, lv, sizeof(slv), 0);
	ss_iovadd(&p->iov, lv, sizeof(slv));
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
		assert(logv->v.i == &sv_vif);
		if (sv_flags(&logv->v) & SVGET)
			continue;
		lv = &lvbuf[lvp];
		sl_write_prepare(p, t, lv, logv);
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

int sl_write(sltx *t, svlog *vlog)
{
	/* assume transaction log is prepared
	 * (lsn set) */
	if (ssunlikely(sv_logcount_write(vlog) == 0))
		return 0;
	if (ssunlikely(! t->p->conf->enable))
		return 0;
	int count = sv_logcount_write(vlog);
	int rc;
	if (sslikely(count == 1)) {
		rc = sl_write_stmt(t, vlog);
	} else {
		svlogv *lv = (svlogv*)vlog->buf.s;
		uint64_t lsn = sv_lsn(&lv->v);
		rc = sl_write_multi_stmt(t, vlog, lsn);
	}
	/* sync */
	if (t->p->conf->enable && t->p->conf->sync_on_write) {
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
