
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
	memset(&n->id, 0, sizeof(n->id));
	n->flags = 0;
	n->recover = 0;
	sd_indexinit(&n->index);
	sr_fileinit(&n->file, r->a);
	sv_indexinit(&n->i0);
	sv_indexinit(&n->i1);
	n->icount = 0;
	n->iused = 0;
	n->iusedkv = 0;
	n->lv = 0;
	n->next = NULL;
	sr_mapinit(&n->map);
	sr_rbinitnode(&n->node);
	sr_rbinitnode(&n->nodemerge);
	sr_rbinitnode(&n->nodebranch);
	return n;
}

int si_nodecreate(sinode *n, siconf *conf,
                  sdid *id,
                  sdindex *i, sdbuild *build)
{
	n->index = *i;
	n->id = *id;
	srpath path;
	sr_path(&path, conf->dir, id->id, ".db.inprogress");
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

static inline int
si_nodeclose(sinode *n, sr *r)
{
	sr_mapunmap(&n->map);
	int rcret = 0;
	int rc = sr_fileclose(&n->file);
	if (srunlikely(rc == -1))
		rcret = -1;
	sd_indexfree(&n->index, r->a);
	sv_indexfree(&n->i0, r);
	sv_indexfree(&n->i1, r);
	return rcret;
}

int si_nodeopen(sinode *n, sr *r, siconf *conf, uint32_t nsn)
{
	srpath path;
	sr_path(&path, conf->dir, nsn, ".db");
	int rc = sr_fileopen(&n->file, path.path);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_mapfile(&n->map, &n->file, 1);
	if (srunlikely(rc == -1))
		goto error;
	rc = sd_indexrecover(&n->index, r->a, &n->map);
	if (srunlikely(rc == -1))
		goto error;
	n->id = n->index.h->id;
	if (srunlikely(n->id.id != nsn))
		goto error;
	return 0;
error:
	si_nodeclose(n, r);
	return -1;
}

int si_nodefree(sinode *n, sr *r)
{
	int rc = si_nodeclose(n, r);
	sr_free(r->a, n);
	return rc;
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

int si_nodeseal(sinode *n, siconf *conf)
{
	srpath path;
	sr_path(&path, conf->dir, n->id.id, ".db");
	int rc = sr_filerename(&n->file, path.path);
	return rc;
}

int si_nodeunlink(siconf *conf, uint32_t id, int inprogress)
{
	char *ext = (inprogress) ? ".db.inprogress" : ".db";
	srpath path;
	sr_path(&path, conf->dir, id, ext);
	int rc = sr_fileunlink(path.path);
	return rc;
}
