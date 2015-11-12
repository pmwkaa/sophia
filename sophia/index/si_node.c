
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
#include <libsd.h>
#include <libsi.h>

sinode *si_nodenew(sr *r)
{
	sinode *n = (sinode*)ss_malloc(r->a, sizeof(sinode));
	if (ssunlikely(n == NULL)) {
		sr_oom_malfunction(r->e);
		return NULL;
	}
	n->recover = 0;
	n->backup = 0;
	n->flags = 0;
	n->update_time = 0;
	n->used = 0;
	si_branchinit(&n->self);
	n->branch = NULL;
	n->branch_count = 0;
	n->temperature = 0;
	n->temperature_reads = 0;
	ss_fileinit(&n->file, r->vfs);
	ss_mmapinit(&n->map);
	ss_mmapinit(&n->map_swap);
	sv_indexinit(&n->i0);
	sv_indexinit(&n->i1);
	ss_rbinitnode(&n->node);
	ss_rqinitnode(&n->nodecompact);
	ss_rqinitnode(&n->nodebranch);
	ss_rqinitnode(&n->nodetemp);
	ss_listinit(&n->commit);
	return n;
}

static inline int
si_nodeclose(sinode *n, sr *r)
{
	int rcret = 0;
	int rc = ss_munmap(&n->map);
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
	sv_indexfree(&n->i0, r);
	sv_indexfree(&n->i1, r);
	return rcret;
}

static inline int
si_noderecover(sinode *n, sr *r, int in_memory)
{
	/* recover branches */
	ssiter i;
	ss_iterinit(sd_recover, &i);
	ss_iteropen(sd_recover, &i, r, &n->file);
	int first = 1;
	int rc;
	while (ss_iteratorhas(&i))
	{
		sdindexheader *h = ss_iteratorof(&i);
		sibranch *b;
		if (first) {
			b =  &n->self;
		} else {
			b = si_branchnew(r);
			if (ssunlikely(b == NULL))
				goto error;
		}
		sdindex index;
		sd_indexinit(&index);
		rc = sd_indexcopy(&index, r, h);
		if (ssunlikely(rc == -1))
			goto error;
		si_branchset(b, &index);

		if (in_memory) {
			char *start = (char*)h - h->total - sizeof(sdseal);
			char *end = start + sizeof(sdseal) + h->total +
			            sizeof(sdindexheader) + h->size +
			            h->extension;
			int branch_size = end - start;
			rc = ss_blobensure(&b->copy, branch_size);
			if (ssunlikely(rc == -1)) {
				sr_oom_malfunction(r->e);
				goto error;
			}
			memcpy(b->copy.p, start, branch_size);
		}

		b->next   = n->branch;
		n->branch = b;
		n->branch_count++;

		first = 0;
		ss_iteratornext(&i);
	}
	rc = sd_recover_complete(&i);
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
	int rc = ss_fileopen(&n->file, path->path);
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
		goto error;
	}
	rc = si_noderecover(n, r, scheme->in_memory);
	if (ssunlikely(rc == -1))
		goto error;
	if (scheme->mmap) {
		rc = si_nodemap(n, r);
		if (ssunlikely(rc == -1))
			goto error;
	}
	return 0;
error:
	si_nodeclose(n, r);
	return -1;
}

int si_nodecreate(sinode *n, sr *r, sischeme *scheme, sdid *id)
{
	sspath path;
	ss_pathAB(&path, scheme->path, id->parent, id->id, ".db.incomplete");
	int rc = ss_filenew(&n->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' create error: %s",
		               path.path, strerror(errno));
		return -1;
	}
	return 0;
}

int si_nodemap(sinode *n, sr *r)
{
	int rc = ss_mmap(&n->map, n->file.fd, n->file.size, 1);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' mmap error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	return 0;
}

static inline void
si_nodefree_branches(sinode *n, sr *r)
{
	sibranch *p = n->branch;
	sibranch *next = NULL;
	while (p && p != &n->self) {
		next = p->next;
		si_branchfree(p, r);
		p = next;
	}
	sd_indexfree(&n->self.index, r);
	ss_blobfree(&n->self.copy);
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
	si_nodefree_branches(n, r);
	rc = si_nodeclose(n, r);
	if (ssunlikely(rc == -1))
		rcret = -1;
	ss_free(r->a, n);
	return rcret;
}

ss_rbtruncate(si_nodegc_indexgc,
              si_gcv((sr*)arg, sscast(n, svv, node)))

int si_nodegc_index(sr *r, svindex *i)
{
	if (i->i.root)
		si_nodegc_indexgc(i->i.root, r);
	sv_indexinit(i);
	return 0;
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

int si_nodeseal(sinode *n, sr *r, sischeme *scheme)
{
	int rc;
	if (scheme->sync) {
		rc = ss_filesync(&n->file);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "db file '%s' sync error: %s",
			               ss_pathof(&n->file.path),
			               strerror(errno));
			return -1;
		}
	}
	sspath path;
	ss_pathAB(&path, scheme->path, n->self.id.parent,
	          n->self.id.id, ".db.seal");
	rc = ss_filerename(&n->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' rename error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
		return -1;
	}
	return 0;
}

int si_nodecomplete(sinode *n, sr *r, sischeme *scheme)
{
	sspath path;
	ss_pathA(&path, scheme->path, n->self.id.id, ".db");
	int rc = ss_filerename(&n->file, path.path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' rename error: %s",
		               ss_pathof(&n->file.path),
		               strerror(errno));
	}
	return rc;
}
