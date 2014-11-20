
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
	if (srunlikely(n == NULL)) {
		sr_error(r->e, "%s", "memory allocation failed");
		return NULL;
	}
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

int si_nodecreate(sinode *n, sr *r, siconf *conf, sdid *id,
                  sdindex *i,
                  sdbuild *build)
{
	n->index = *i;
	n->id = *id;
	srpath path;
	sr_pathA(&path, conf->path, id->id, ".db.incomplete");
	int rc = sr_filenew(&n->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' create error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	rc = sd_buildwrite(build, &n->index, &n->file);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_mapfile(&n->map, &n->file, 1);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' map error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	return 0;
}

int
si_nodecreate_attach(sinode *n, sr *r, siconf *conf, sdid *id,
                     sdindex *i,
                     sdbuild *build)
{
	n->index = *i;
	n->id = *id;
	srpath path;
	sr_pathAB(&path, conf->path, id->parent, id->id, ".db.incomplete");
	int rc = sr_filenew(&n->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' create error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	rc = sd_buildwrite(build, &n->index, &n->file);
	if (srunlikely(rc == -1))
		return -1;
	rc = sr_mapfile(&n->map, &n->file, 1);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' map error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	return 0;
}

static inline int
si_nodeclose(sinode *n, sr *r)
{
	sr_mapunmap(&n->map);
	int rcret = 0;
	int rc = sr_fileclose(&n->file);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' close error: %s",
		         n->file.file, strerror(errno));
		rcret = -1;
	}
	sd_indexfree(&n->index, r);
	sv_indexfree(&n->i0, r);
	sv_indexfree(&n->i1, r);
	return rcret;
}

int si_nodeopen(sinode *n, sr *r, srpath *path)
{
	int rc = sr_fileopen(&n->file, path->path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' open error: %s",
		         n->file.file, strerror(errno));
		return -1;
	}
	rc = sr_mapfile(&n->map, &n->file, 1);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' map error: %s",
		         n->file.file, strerror(errno));
		goto error;
	}
	rc = sd_indexrecover(&n->index, r, &n->map);
	if (srunlikely(rc == -1))
		goto error;
	n->id = n->index.h->id;
	return 0;
error:
	si_nodeclose(n, r);
	return -1;
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
			if (srunlikely(rc == -1)) {
				sr_error(r->e, "db file '%s' unlink error: %s",
				         p->file.file, strerror(errno));
				rcret = -1;
			}
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

int si_nodeseal(sinode *n, sr *r, siconf *conf)
{
	srpath path;
	sr_pathAB(&path, conf->path, n->id.parent, n->id.id, ".db.seal");
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
	sr_pathA(&path, conf->path, n->id.id, ".db");
	int rc = sr_filerename(&n->file, path.path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "db file '%s' rename error: %s",
				 n->file.file, strerror(errno));
	}
	return rc;
}
