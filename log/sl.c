
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsl.h>

static inline sl*
sl_alloc(slpool *p, uint32_t id)
{
	sl *l = sr_malloc(p->r->a, sizeof(*l));
	if (srunlikely(l == NULL)) {
		sr_error(p->r->e, "%s", "memory allocation failed");
		return NULL;
	}
	l->id   = id;
	l->used = 0;
	l->p    = NULL;
	sr_gcinit(&l->gc);
	sr_mutexinit(&l->filelock);
	sr_fileinit(&l->file, p->r->a);
	sr_listinit(&l->link);
	return l;
}

static inline int
sl_close(slpool *p, sl *l)
{
	int rc = sr_fileclose(&l->file);
	if (srunlikely(rc == -1)) {
		sr_error(p->r->e, "log file '%s' close error: %s",
		         l->file.file, strerror(errno));
	}
	sr_mutexfree(&l->filelock);
	sr_gcfree(&l->gc);
	sr_free(p->r->a, l);
	return rc;
}

static inline sl*
sl_open(slpool *p, uint32_t id)
{
	sl *l = sl_alloc(p, id);
	if (srunlikely(l == NULL))
		return NULL;
	srpath path;
	sr_pathA(&path, p->conf->dir, id, ".log");
	int rc = sr_fileopen(&l->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(p->r->e, "log file '%s' open error: %s",
		         l->file.file, strerror(errno));
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
	if (srunlikely(l == NULL))
		return NULL;
	srpath path;
	sr_pathA(&path, p->conf->dir, id, ".log");
	int rc = sr_filenew(&l->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(p->r->e, "log file '%s' create error: %s",
		         l->file.file, strerror(errno));
		goto error;
	}
	srversion v;
	sr_version(&v);
	rc = sr_filewrite(&l->file, &v, sizeof(v));
	if (srunlikely(rc == -1)) {
		sr_error(p->r->e, "log file '%s' header write error: %s",
		         l->file.file, strerror(errno));
		goto error;
	}
	return l;
error:
	sl_close(p, l);
	return NULL;
}

int sl_poolinit(slpool *p, sr *r, slconf *conf)
{
	sr_spinlockinit(&p->lock);
	sr_listinit(&p->list);
	p->n       = 0;
	p->r       = r;
	p->conf    = conf;
	p->enabled = 1;
	struct iovec *iov =
		sr_malloc(r->a, sizeof(struct iovec) * 1021);
	if (srunlikely(iov == NULL))
		return sr_error(r->e, "%s", "memory allocation failed");
	sr_iovinit(&p->iov, iov, 1021);
	if (conf->dir == NULL)
		p->enabled = 0;
	return 0;
}

static inline int
sl_poolcreate(slpool *p)
{
	int rc;
	if (! p->conf->dir_create) {
		sr_error(p->r->e, "log directory '%s' can't be created",
		         p->conf->dir);
		sr_error_recoverable(p->r->e);
		return -1;
	}
	if (! p->conf->dir_write) {
		sr_error(p->r->e, "log directory '%s' is read only",
		         p->conf->dir);
		sr_error_recoverable(p->r->e);
		return -1;
	}
	rc = sr_filemkdir(p->conf->dir);
	if (srunlikely(rc == -1))
		return sr_error(p->r->e, "log directory '%s' create error: %s",
		                p->conf->dir, strerror(errno));
	return 1;
}

static inline int
sl_poolrecover(slpool *p)
{
	srbuf list;
	sr_bufinit(&list);
	srdirtype types[] =
	{
		{ "log", 1, 0 },
		{ NULL,  0, 0 }
	};
	int rc = sr_dirread(&list, p->r->a, types, p->conf->dir);
	if (srunlikely(rc == -1))
		return sr_error(p->r->e, "log directory '%s' open error",
		                p->conf->dir);
	sriter i;
	sr_iterinit(&i, &sr_bufiter, p->r);
	sr_iteropen(&i, &list, sizeof(srdirid));
	while(sr_iterhas(&i)) {
		srdirid *id = sr_iterof(&i);
		sl *l = sl_open(p, id->id);
		if (srunlikely(l == NULL)) {
			sr_buffree(&list, p->r->a);
			return -1;
		}
		sr_listappend(&p->list, &l->link);
		p->n++;
		sr_iternext(&i);
	}
	sr_buffree(&list, p->r->a);
	if (p->n) {
		sl *last = srcast(p->list.prev, sl, link);
		p->r->seq->lfsn = last->id;
		p->r->seq->lfsn++;
	}
	return 0;
}

int sl_poolopen(slpool *p)
{
	if (srunlikely(! p->enabled))
		return 0;
	int exists = sr_fileexists(p->conf->dir);
	int rc;
	if (! exists)
		rc = sl_poolcreate(p);
	else
		rc = sl_poolrecover(p);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

int sl_poolrotate(slpool *p)
{
	if (srunlikely(! p->enabled))
		return 0;
	uint32_t lfsn = sr_seq(p->r->seq, SR_LFSNNEXT);
	sl *l = sl_new(p, lfsn);
	if (srunlikely(l == NULL))
		return -1;
	sl *log = NULL;
	sr_spinlock(&p->lock);
	if (p->n) {
		log = srcast(p->list.prev, sl, link);
		sr_gccomplete(&log->gc);
	}
	sr_listappend(&p->list, &l->link);
	p->n++;
	sr_spinunlock(&p->lock);
	if (log) {
		if (p->conf->sync_on_rotate) {
			int rc = sr_filesync(&log->file);
			if (srunlikely(rc == -1)) {
				sr_error(p->r->e, "log file '%s' sync error: %s",
						 log->file.file, strerror(errno));
				return -1;
			}
		}
	}
	return 0;
}

int sl_poolrotate_ready(slpool *p, int wm)
{
	if (srunlikely(! p->enabled))
		return 0;
	sr_spinlock(&p->lock);
	assert(p->n > 0);
	sl *l = srcast(p->list.prev, sl, link);
	int ready = sr_gcrotateready(&l->gc, wm);
	sr_spinunlock(&p->lock);
	return ready;
}

int sl_poolshutdown(slpool *p)
{
	int rcret = 0;
	int rc;
	if (p->n) {
		srlist *i, *n;
		sr_listforeach_safe(&p->list, i, n) {
			sl *l = srcast(i, sl, link);
			rc = sl_close(p, l);
			if (srunlikely(rc == -1))
				rcret = -1;
		}
	}
	if (p->iov.v)
		sr_free(p->r->a, p->iov.v);
	sr_spinlockfree(&p->lock);
	return rcret;
}

static inline int
sl_gc(slpool *p, sl *l)
{
	int rc;
	rc = sr_fileunlink(l->file.file);
	if (srunlikely(rc == -1)) {
		return sr_error(p->r->e, "log file '%s' unlink error: %s",
		                l->file.file, strerror(errno));
	}
	rc = sl_close(p, l);
	if (srunlikely(rc == -1))
		return -1;
	return 1;
}

int sl_poolgc(slpool *p)
{
	if (srunlikely(! p->enabled))
		return 0;
	for (;;) {
		sr_spinlock(&p->lock);
		sl *current = NULL;
		srlist *i;
		sr_listforeach(&p->list, i) {
			sl *l = srcast(i, sl, link);
			if (srlikely(! sr_gcgarbage(&l->gc)))
				continue;
			sr_listunlink(&l->link);
			p->n--;
			current = l;
			break;
		}
		sr_spinunlock(&p->lock);
		if (current) {
			int rc = sl_gc(p, current);
			if (srunlikely(rc == -1))
				return -1;
		} else {
			break;
		}
	}
	return 0;
}

int sl_begin(slpool *p, sltx *t)
{
	memset(t, 0, sizeof(*t));
	sr_spinlock(&p->lock);
	t->p = p;
	if (! p->enabled)
		return 0;
	assert(p->n > 0);
	sl *l = srcast(p->list.prev, sl, link);
	sr_mutexlock(&l->filelock);
	t->svp = sr_filesvp(&l->file);
	t->l = l;
	t->p = p;
	return 0;
}

int sl_commit(sltx *t)
{
	if (t->p->enabled)
		sr_mutexunlock(&t->l->filelock);
	sr_spinunlock(&t->p->lock);
	return 0;
}

int sl_rollback(sltx *t)
{
	int rc = 0;
	if (t->p->enabled) {
		rc = sr_filerlb(&t->l->file, t->svp);
		if (srunlikely(rc == -1))
			sr_error(t->p->r->e, "log file '%s' truncate error: %s",
			         t->l->file.file, strerror(errno));
		sr_mutexunlock(&t->l->filelock);
	}
	sr_spinunlock(&t->p->lock);
	return rc;
}

static inline int
sl_write_disabled(sltx *t, svlog *vlog, uint64_t lsn)
{
	slpool *p = t->p;
	sriter i;
	sr_iterinit(&i, &sr_bufiter, p->r);
	sr_iteropen(&i, &vlog->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sv *v = sr_iterof(&i);
		((svv*)v->v)->log = NULL;
		svlsnset(v, lsn);
	}
	return 0;
}

int sl_write(sltx *t, svlog *vlog)
{
	slpool *p = t->p;
	sl *l = t->l;
	int rc;
	uint64_t lsn = sr_seq(p->r->seq, SR_LSNNEXT);
	if (srunlikely(! p->enabled))
		return sl_write_disabled(t, vlog, lsn);

	slv lvbuf[341]; /* 1 + 340 per syscall */
	int lvp;
	lvp = 0;

	/* begin */
	slv *lv = &lvbuf[0];
	lv->lsn       = lsn;
	lv->flags     = SVBEGIN;
	lv->valuesize = sv_logn(vlog);
	lv->keysize   = 0;
	lv->crc       = sr_crcs(lv, sizeof(slv), 0);
	sr_iovadd(&p->iov, lv, sizeof(slv));
	lvp++;

	sriter i;
	sr_iterinit(&i, &sr_bufiter, p->r);
	sr_iteropen(&i, &vlog->buf, sizeof(sv));
	for (; sr_iterhas(&i); sr_iternext(&i))
	{
		sv *v = sr_iterof(&i);
		svlsnset(v, lsn);

		if (srunlikely(! sr_iovensure(&p->iov, 3))) {
			rc = sr_filewritev(&l->file, &p->iov);
			if (srunlikely(rc == -1)) {
				sr_error(p->r->e, "log file '%s' write error: %s",
				         l->file.file, strerror(errno));
				return -1;
			}
			sr_iovreset(&p->iov);
			lvp = 0;
		}
		/* prepare header */
		lv = &lvbuf[lvp];
		lv->lsn       = lsn;
		lv->flags     = svflags(v);
		lv->valuesize = svvaluesize(v);
		lv->keysize   = svkeysize(v);
		lv->crc       = sr_crcp(svkey(v), lv->keysize, 0);
		lv->crc       = sr_crcp(svvalue(v), lv->valuesize, lv->crc);
		lv->crc       = sr_crcs(lv, sizeof(slv), lv->crc);
		/* prepare to write */
		sr_iovadd(&p->iov, lv, sizeof(slv));
		sr_iovadd(&p->iov, svkey(v), lv->keysize);
		sr_iovadd(&p->iov, svvalue(v), lv->valuesize);
		lvp++;
	}
	if (srlikely(sr_iovhas(&p->iov))) {
		rc = sr_filewritev(&l->file, &p->iov);
		if (srunlikely(rc == -1)) {
			sr_error(p->r->e, "log file '%s' write error: %s",
			         l->file.file, strerror(errno));
			return -1;
		}
		sr_iovreset(&p->iov);
	}
	sr_gcmark(&l->gc, sv_logn(vlog));

	/* sync */
	if (p->conf->sync_on_write) {
		rc = sr_filesync(&l->file);
		if (srunlikely(rc == -1)) {
			sr_error(p->r->e, "log file '%s' sync error: %s",
					 l->file.file, strerror(errno));
			return -1;
		}
	}
	return 0;
}
