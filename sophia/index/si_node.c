
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
#include <libsw.h>
#include <libsd.h>
#include <libsi.h>

sinode *si_nodenew(sr *r, uint64_t id, uint64_t id_parent)
{
	sinode *n = (sinode*)ss_malloc(r->a, sizeof(sinode));
	if (ssunlikely(n == NULL)) {
		sr_oom_malfunction(r->e);
		return NULL;
	}
	n->id        = id;
	n->id_parent = id_parent;
	n->recover   = 0;
	n->backup    = 0;
	n->flags     = 0;
	n->used      = 0;
	n->refs      = 0;
	ss_spinlockinit(&n->reflock);
	sd_indexinit(&n->index);
	ss_fileinit(&n->file, r->vfs);
	ss_mmapinit(&n->map);
	ss_mmapinit(&n->map_swap);
	sv_indexinit(&n->i0);
	sv_indexinit(&n->i1);
	ss_rbinitnode(&n->node);
	ss_rqinitnode(&n->nodememory);
	ss_listinit(&n->gc);
	ss_listinit(&n->commit);
	return n;
}

ss_rbtruncate(si_nodegc_indexgc,
              si_gcvall((sr*)arg, sscast(n, svv, node)))

int si_nodegc_index(sr *r, svindex *i)
{
	if (i->i.root)
		si_nodegc_indexgc(i->i.root, r);
	sv_indexinit(i);
	return 0;
}

static inline int
si_nodeclose(sinode *n, sr *r, int gc)
{
	int rcret = 0;

	int rc = ss_vfsmunmap(r->vfs, &n->map);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' munmap error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		rcret = -1;
	}
	rc = ss_fileclose(&n->file);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' close error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		rcret = -1;
	}
	if (gc) {
		si_nodegc_index(r, &n->i0);
		si_nodegc_index(r, &n->i1);
	} else {
		sv_indexfree(&n->i0, r);
		sv_indexfree(&n->i1, r);
		ss_spinlockfree(&n->reflock);
	}
	return rcret;
}

static inline int
si_noderecover(sinode *n, sr *r)
{
	int rc;
	ssiter i;
	ss_iterinit(sd_iter, &i);
	rc = ss_iteropen(sd_iter, &i, r, &n->file);
	if (ssunlikely(rc == -1))
		return -1;

	while (ss_iteratorhas(&i))
	{
		sdindexheader *h = ss_iteratorof(&i);

		sdindex index;
		sd_indexinit(&index);
		rc = sd_indexcopy(&index, r, h);
		if (ssunlikely(rc == -1))
			goto error;
		n->index = index;

		ss_iteratornext(&i);
	}

	rc = sd_iter_iserror(&i);
	if (ssunlikely(rc == -1))
		goto error;
	ss_iteratorclose(&i);
	return 0;

error:
	ss_iteratorclose(&i);
	return -1;
}

int si_nodeopen(sinode *n, sr *r, sischeme *scheme, sspath *path)
{
	int rc = ss_fileopen(&n->file, path->path, scheme->direct_io);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' open error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	rc = ss_fileseek(&n->file, n->file.size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' seek error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	rc = si_noderecover(n, r);
	if (ssunlikely(rc == -1))
		return -1;
	if (scheme->mmap) {
		rc = si_nodemap(n, r);
		if (ssunlikely(rc == -1))
			return -1;
	}
	return 0;
}

int si_nodecreate(sinode *n, sr *r, sischeme *scheme)
{
	sspath path;
	ss_pathcompound(&path, scheme->path, n->id_parent, n->id,
	                ".db.incomplete");
	int rc = ss_filenew(&n->file, path.path, scheme->direct_io);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' create error: %s",
		               path.path, strerror(errno));
		return -1;
	}
	return 0;
}

int si_nodemap(sinode *n, sr *r)
{
	int rc = ss_vfsmmap(r->vfs, &n->map, n->file.fd, n->file.size, 1);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' mmap error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	return 0;
}

int si_nodefree(sinode *n, sr *r, int gc)
{
	int rcret = 0;
	int rc;
	if (gc && ss_pathis_set(&n->file.path)) {
		ss_fileadvise(&n->file, 0, 0, n->file.size);
		rc = ss_vfsunlink(r->vfs, ss_pathof(&n->file.path));
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "db file '%s' unlink error: %s",
			               ss_pathof(&n->file.path),
			               strerror(errno));
			rcret = -1;
		}
	}
	sd_indexfree(&n->index, r);
	rc = si_nodeclose(n, r, gc);
	if (ssunlikely(rc == -1))
		rcret = -1;
	ss_free(r->a, n);
	return rcret;
}

int si_noderead(sinode *n, sr *r, ssbuf *dest)
{
	int rc = ss_bufensure(dest, r->a, n->file.size);
	if (ssunlikely(rc == -1))
		return sr_oom_malfunction(r->e);
	rc = ss_filepread(&n->file, 0, dest->s, n->file.size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' read error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	ss_bufadvance(dest, n->file.size);
	return 0;
}

int si_noderename_seal(sinode *n, sr *r, sischeme *scheme)
{
	int rc;
	sspath path;
	ss_pathcompound(&path, scheme->path, n->id_parent, n->id,
	                ".db.seal");
	rc = ss_filerename(&n->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' rename error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	return 0;
}

int si_noderename_complete(sinode *n, sr *r, sischeme *scheme)
{
	sspath path;
	ss_path(&path, scheme->path, n->id, ".db");
	int rc = ss_filerename(&n->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' rename error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
	}
	return rc;
}

int si_nodegc(sinode *n, sr *r, sischeme *scheme)
{
	sspath path;
	ss_path(&path, scheme->path, n->id, ".db.gc");
	int rc = ss_filerename(&n->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' rename error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
	}
	return rc;
}
