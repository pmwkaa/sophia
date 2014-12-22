
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
#include <libsd.h>
#include <libsi.h>

sinode *si_nodenew(sr *r)
{
	sinode *n = (sinode*)sr_malloc(r->a, sizeof(sinode));
	if (srunlikely(n == NULL)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return NULL;
	}
	n->recover = 0;
	n->flags = 0;
	n->update_time = 0;
	n->used = 0;
	si_branchinit(&n->self);
	n->branch = NULL;
	n->branch_count = 0;
	sr_fileinit(&n->file, r->a);
	sv_indexinit(&n->i0);
	sv_indexinit(&n->i1);
	sr_rbinitnode(&n->node);
	sr_rbinitnode(&n->nodecompact);
	sr_rbinitnode(&n->nodebranch);
	return n;
}

static inline int
si_nodeclose(sinode *n, sr *r)
{
	int rcret = 0;
	int rc = sr_fileclose(&n->file);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' close error: %s",
		         n->file.file, strerror(errno));
		rcret = -1;
	}
	sv_indexfree(&n->i0, r);
	sv_indexfree(&n->i1, r);
	return rcret;
}

static inline int
si_noderecover(sinode *n, sr *r)
{
	/* recover branches */
	sriter i;
	sr_iterinit(&i, &sd_recover, r);
	sr_iteropen(&i, &n->file);
	int first = 1;
	int rc;
	while (sr_iterhas(&i)) {
		sdindexheader *h = sr_iterof(&i);
		sibranch *b;
		if (first) {
			b =  &n->self;
		} else {
			b = si_branchnew(r);
			if (srunlikely(b == NULL))
				goto error;
		}
		sdindex index;
		sd_indexinit(&index);
		rc = sd_indexcopy(&index, r, h);
		if (srunlikely(rc == -1))
			goto error;
		si_branchset(b, &index);

		b->next   = n->branch;
		n->branch = b;
		n->branch_count++;

		first = 0;
		sr_iternext(&i);
	}
	rc = sd_recovercomplete(&i);
	if (srunlikely(rc == -1))
		goto error;
	sr_iterclose(&i);
	return 0;
error:
	sr_iterclose(&i);
	return -1;
}

int si_nodeopen(sinode *n, sr *r, srpath *path)
{
	int rc = sr_fileopen(&n->file, path->path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' open error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	rc = sr_fileseek(&n->file, n->file.size);
	if (srunlikely(rc == -1)) {
		si_nodeclose(n, r);
		sr_error(r->e, "db file '%s' seek error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	rc = si_noderecover(n, r);
	if (srunlikely(rc == -1))
		si_nodeclose(n, r);
	return rc;
}

int si_nodecreate(sinode *n, sr *r, siconf *conf, sdid *id,
                  sdindex *i,
                  sdbuild *build)
{
	si_branchset(&n->self, i);
	srpath path;
	sr_pathAB(&path, conf->path, id->parent, id->id, ".db.incomplete");
	int rc = sr_filenew(&n->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' create error: %s",
		         path.path, strerror(errno));
		return -1;
	}
	rc = sd_buildwrite(build, &n->self.index, &n->file);
	if (srunlikely(rc == -1))
		return -1;
	n->branch = &n->self;
	n->branch_count++;
	return 0;
}

int si_nodesync(sinode *n, sr *r)
{
	int rc = sr_filesync(&n->file);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' sync error: %s",
		         n->file.file, strerror(errno));
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
}

int si_nodefree(sinode *n, sr *r, int gc)
{
	int rcret = 0;
	int rc;
	if (gc && n->file.file) {
		rc = sr_fileunlink(n->file.file);
		if (srunlikely(rc == -1)) {
			sr_error(r->e, "db file '%s' unlink error: %s",
			         n->file.file, strerror(errno));
			rcret = -1;
		}
	}
	si_nodefree_branches(n, r);
	rc = si_nodeclose(n, r);
	if (srunlikely(rc == -1))
		rcret = -1;
	sr_free(r->a, n);
	return rcret;
}

uint32_t si_vgc(sra*, svv*);

sr_rbtruncate(si_nodegc_indexgc,
              si_vgc((sra*)arg, srcast(n, svv, node)))

int si_nodegc_index(sr *r, svindex *i)
{
	if (i->i.root)
		si_nodegc_indexgc(i->i.root, r->a);
	sv_indexinit(i);
	return 0;
}

int si_nodecmp(sinode *n, void *key, int size, srcomparator *c)
{
	sdindexpage *min = sd_indexmin(&n->self.index);
	sdindexpage *max = sd_indexmax(&n->self.index);
	int l = sr_compare(c, sd_indexpage_min(min), min->sizemin, key, size);
	int r = sr_compare(c, sd_indexpage_max(max), max->sizemin, key, size);
	/* inside range */
	if (l <= 0 && r >= 0)
		return 0;
	/* key > range */
	if (l == -1)
		return -1;
	/* key < range */
	assert(r == 1);
	return 1;
}

int si_nodeseal(sinode *n, sr *r, siconf *conf)
{
	srpath path;
	sr_pathAB(&path, conf->path, n->self.id.parent,
	          n->self.id.id, ".db.seal");
	int rc = sr_filerename(&n->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' rename error: %s",
				 n->file.file, strerror(errno));
	}
	return rc;
}

int si_nodecomplete(sinode *n, sr *r, siconf *conf)
{
	srpath path;
	sr_pathA(&path, conf->path, n->self.id.id, ".db");
	int rc = sr_filerename(&n->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' rename error: %s",
				 n->file.file, strerror(errno));
	}
	return rc;
}
