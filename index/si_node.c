
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
#include <libsi.h>

sinode *si_nodenew(sr *r)
{
	sinode *n = (sinode*)sr_malloc(r->a, sizeof(sinode));
	if (srunlikely(n == NULL))
		return NULL;
	n->id = 0;
	n->flags = 0;
	n->recover = 0;
	sd_indexinit(&n->index);
	sr_fileinit(&n->file, r->a);
	sv_indexinit(&n->i0);
	sv_indexinit(&n->i1);
	n->icount = 0;
	n->iused = 0;
	n->lv = 0;
	n->next = NULL;
	sr_mapinit(&n->map);
	sr_rbinitnode(&n->node);
	sr_rbinitnode(&n->nodemerge);
	sr_rbinitnode(&n->nodebranch);
	return n;
}

int si_nodecreate(sinode *n, siconf *conf,
                  sinode *parent,
                  sdindex *i, sdbuild *build)
{
	n->index = *i;
	srpath path;
	if (parent == NULL) {
		sr_pathA(&path, conf->dir, n->id, ".db.inprogress");
	} else {
		sr_pathAB(&path, conf->dir, parent->id, n->id, ".db.inprogress");
	}
	int rc = sr_filenew(&n->file, path.path);
	if (srunlikely(rc == -1))
		return -1;
	rc = sd_buildwrite(build, &n->index, &n->file);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_mapfile(&n->map, &n->file, 1);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

int si_nodeopen(sinode *n, sr *r, siconf *conf, sinode *parent)
{
	srpath path;
	if (parent == NULL) {
		sr_pathA(&path, conf->dir, n->id, ".db");
	} else {
		sr_pathAB(&path, conf->dir, parent->id, n->id, ".db");
	}
	int rc = sr_fileopen(&n->file, path.path);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_mapfile(&n->map, &n->file, 1);
	if (srunlikely(rc == -1))
		return -1;
	rc = sd_indexrecover(&n->index, r->a, &n->map);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

int si_nodefree(sinode *n, sr *r)
{
	sr_mapunmap(&n->map);
	int rcret = 0;
	int rc = sr_fileclose(&n->file);
	if (srunlikely(rc == -1))
		rcret = -1;
	sd_indexfree(&n->index, r->a);
	sv_indexfree(&n->i0, r);
	sv_indexfree(&n->i1, r);
	sr_free(r->a, n);
	return rcret;
}

int si_nodefree_all(sinode *n, sr *r)
{
	int rcret = 0;
	int rc;
	sinode *next = NULL;
	sinode *p = n;
	while (p) {
		next = p->next;
		rc = si_nodefree(p, r);
		if (srunlikely(rc == -1))
			rcret = -1;
		p = next;
	}
	return rcret;
}

int si_nodegc(sinode *n, sr *r)
{
	int rcret = 0;
	int rc;
	sinode *next = NULL;
	sinode *p = n;
	while (p) {
		next = p->next;
		if (p->file.file) {
			rc = sr_fileunlink(p->file.file);
			if (srunlikely(rc == -1))
				rcret = -1;
		}
		rc = si_nodefree(p, r);
		if (srunlikely(rc == -1))
			rcret = -1;
		p = next;
	}
	return rcret;
}

int si_nodecmp(sinode *n, void *key, int size, srcomparator *c)
{
	sdindexpage *min = sd_indexmin(&n->index);
	sdindexpage *max = sd_indexmax(&n->index);
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

int si_nodeseal(sinode *n, siconf *conf, sinode *parent)
{
	srpath path;
	if (parent == NULL) {
		sr_pathA(&path, conf->dir, n->id, ".db");
	} else {
		sr_pathAB(&path, conf->dir, parent->id, n->id, ".db");
	}
	int rc = sr_filerename(&n->file, path.path);
	return rc;
}

int si_nodeunlink(sinode *n, siconf *conf, sinode *parent, int inprogress)
{
	char *ext = (inprogress) ? ".db.inprogress" : ".db";
	srpath path;
	if (parent == NULL) {
		sr_pathA(&path, conf->dir, n->id, ext);
	} else {
		sr_pathAB(&path, conf->dir, parent->id, n->id, ext);
	}
	int rc = sr_fileunlink(path.path);
	return rc;
}
