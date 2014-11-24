
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/*
	repository recover states
	-------------------------

	I. branch
	000000001.db.incomplete (1) (2)
	000000001.db            (3)

	1. remove incomplete node
	2. incomplete node without parent is corrupt
	3. link branch node with head

	II. compaction
	000000001.000000002.db.incomplete  (1)
	000000001.000000002.db.seal        (2)
	000000002.db                       (3)
	000000001.000000003.db.incomplete
	000000001.000000003.db.seal
	000000003.db
	(4)

	1. remove incomplete, mark parent as having incomplete
	2. find parent, mark as having seal
	3. add
	4. recover:
		a. if parent has incomplete and seal - remove both
		b. if parent has incomplete - remove incomplete
		c. if parent has seal - remove parent, complete seal

	see: test/recovery_crash.test.c
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>
#include <si_track.h>

static inline int
si_deploy(si *i, sr *r)
{
	int rc;
	rc = sr_filemkdir(i->conf->path);
	if (srunlikely(rc == -1)) {
		sr_error(r->e, "directory '%s' create error: %s",
		         i->conf->path, strerror(errno));
		return -1;
	}
	sr_seq(r->seq, SR_LSNNEXT);
	sinode *n = si_nodenew(r);
	if (srunlikely(n == NULL))
		return -1;
	sdid id = {
		.parent = 0,
		.flags  = 0,
		.id     = sr_seq(r->seq, SR_NSNNEXT)
	};
	n->id = id;
	sdindex index;
	sd_indexinit(&index);
	rc = sd_indexbegin(&index, r, 0);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	rc = sd_indexadd(&index, r, 0, 0, 0, 0, "", 1, "", 1, 0, 0);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r);
		si_nodefree(n, r);
		return -1;
	}
	sd_indexcommit(&index, r, &id);
	sdbuild build;
	sd_buildinit(&build, r);
	rc = sd_buildbegin(&build, 0);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r);
		sd_buildfree(&build);
		si_nodefree(n, r);
		return -1;
	}
	sd_buildend(&build);
	sd_buildcommit(&build);
	rc = si_nodecreate(n, r, i->conf, &id, &index, &build);
	sd_buildfree(&build);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	rc = si_nodecomplete(n, r, i->conf);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	si_insert(i, r, n);
	return 1;
}

static inline ssize_t
si_processid(char **str) {
	char *s = *str;
	size_t v = 0;
	while (*s && *s != '.') {
		if (srunlikely(!isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	*str = s;
	return v;
}

static inline int
si_process(char *name, uint32_t *nsn, uint32_t *parent)
{
	/* id.db */
	/* id.db.incomplete */
	/* id.id.db.incomplete */
	/* id.id.db.seal */
	char *token = name;
	ssize_t id = si_processid(&token);
	if (srunlikely(id == -1))
		return -1;
	*parent = id;
	*nsn = id;
	if (strcmp(token, ".db") == 0)
		return SI_RDB;
	else
	if (strcmp(token, ".db.incomplete") == 0)
		return SI_RDBI;
	if (srunlikely(*token != '.'))
		return -1;
	token++;
	id = si_processid(&token);
	if (srunlikely(id == -1))
		return -1;
	*nsn = id;
	if (strcmp(token, ".db.incomplete") == 0)
		return SI_RDB_DBI;
	else
	if (strcmp(token, ".db.seal") == 0)
		return SI_RDB_DBSEAL;
	return -1;
}

static inline int
si_trackdir(sitrack *track, sr *r, si *i)
{
	DIR *dir = opendir(i->conf->path);
	if (srunlikely(dir == NULL)) {
		sr_error(r->e, "directory '%s' open error: %s",
		         i->conf->path, strerror(errno));
		return -1;
	}
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (srunlikely(de->d_name[0] == '.'))
			continue;
		uint32_t id_parent = 0;
		uint32_t id = 0;
		int rc = si_process(de->d_name, &id, &id_parent);
		if (srunlikely(rc == -1))
			continue; /* skip unknown file */
		si_tracknsn(track, id_parent);
		si_tracknsn(track, id);

		sinode *head, *node;
		srpath path;
		switch (rc) {
		case SI_RDBI:
			/* remove any incomplete branch */
			sr_pathA(&path, i->conf->path, id, ".db.incomplete");
			rc = sr_fileunlink(path.path);
			if (srunlikely(rc == -1)) {
				sr_error(r->e, "db file '%s' unlink error: %s",
				         path.path, strerror(errno));
				goto error;
			}
			continue;
		case SI_RDB_DBI:
		case SI_RDB_DBSEAL: {
			/* find parent node and mark it as having
			 * incomplete compaction process */
			head = si_trackget(track, id_parent);
			if (srlikely(head == NULL)) {
				head = si_nodenew(r);
				if (srunlikely(head == NULL))
					goto error;
				head->id.id = id_parent;
				head->recover = SI_RDB_UNDEF;
				si_trackset(track, head);
			}
			head->recover |= rc;
			/* remove any incomplete file made during compaction */
			if (rc == SI_RDB_DBI) {
				sr_pathAB(&path, i->conf->path, id_parent, id, ".db.incomplete");
				rc = sr_fileunlink(path.path);
				if (srunlikely(rc == -1)) {
					sr_error(r->e, "db file '%s' unlink error: %s",
					         path.path, strerror(errno));
					goto error;
				}
				continue;
			}
			assert(rc == SI_RDB_DBSEAL);
			/* recover 'sealed' node */
			node = si_nodenew(r);
			if (srunlikely(node == NULL))
				goto error;
			node->recover = SI_RDB_DBSEAL;
			sr_pathAB(&path, i->conf->path, id_parent, id, ".db.seal");
			rc = si_nodeopen(node, r, &path);
			if (srunlikely(rc == -1)) {
				si_nodefree(node, r);
				goto error;
			}
			si_trackset(track, node);
			si_tracklsn(track, node);
			continue;
		}
		}
		assert(rc == SI_RDB);

		/* recover node */
		node = si_nodenew(r);
		if (srunlikely(node == NULL))
			goto error;
		node->recover = SI_RDB;
		sr_pathA(&path, i->conf->path, id, ".db");
		rc = si_nodeopen(node, r, &path);
		if (srunlikely(rc == -1)) {
			si_nodefree(node, r);
			goto error;
		}
		si_tracklsn(track, node);

		/* search previous definition or an older
		 * version */
		if (node->id.flags & SD_IDBRANCH) {
			head = si_trackget(track, node->id.parent);
			if (srlikely(head == NULL)) {
				head = si_nodenew(r);
				if (srunlikely(head == NULL))
					goto error;
				head->id.id = node->id.parent;
				head->recover = SI_RDB_UNDEF;
				si_trackset(track, head);
			}
			node->next = head->next;
			head->next = node;
			head->lv++;
			continue;
		}

		head = si_trackget(track, id);
		if (srlikely(head == NULL)) {
			si_trackset(track, node);
		} else {
			/* replace a node previously created
			 * by a branch */
			if (! (head->recover & SI_RDB_UNDEF))
				goto error;
			head->recover &= ~SI_RDB_UNDEF;
			si_trackreplace(track, head, node);
			node->recover |= head->recover;
			node->next     = head->next;
			node->lv       = head->lv;
			head->next     = NULL;
			si_nodefree(head, r);
		}
	}
	closedir(dir);
	return 0;
error:
	closedir(dir);
	return -1;
}

static int
si_branchcmp(const void *p1, const void *p2)
{
	sinode *a = *(sinode**)p1;
	sinode *b = *(sinode**)p2;
	if (a->id.id == b->id.id)
		return 0;
	return (a->id.id > b->id.id) ? 1 : -1;
}

static inline int
si_branchsort(sr *r, srbuf *buf, sinode *parent)
{
	if (parent->lv == 0)
		return 0;
	sr_bufreset(buf);
	int rc;
	rc = sr_bufensure(buf, r->a, sizeof(sinode*) * parent->lv);
	if (srunlikely(rc == -1))
		return sr_error(r->e, "%s", "memory allocation failed");
	sinode *n = parent->next;
	while (n) {
		sr_bufadd(buf, r->a, &n, sizeof(sinode*));
		n = n->next;
	}
	uint32_t count = sr_bufused(buf) / sizeof(sinode*);
	assert(count == parent->lv);
	qsort(buf->s, count, sizeof(sinode*), si_branchcmp);
	parent->next = NULL;
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, r);
	sr_iteropen(&i, buf, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sinode *n = sr_iterof(&i);
		n->next = parent->next;
		parent->next = n;
	}
	return 0;
}

static inline int
si_trackvalidate(sitrack *track, srbuf *buf, sr *r, si *i)
{
	sr_bufreset(buf);
	srrbnode *p = sr_rbmax(&track->i);
	while (p) {
		sinode *n = srcast(p, sinode, node);
		switch (n->recover) {
		case SI_RDB|SI_RDB_DBI|SI_RDB_DBSEAL|SI_RDB_REMOVE:
		case SI_RDB|SI_RDB_DBSEAL|SI_RDB_REMOVE:
		case SI_RDB|SI_RDB_REMOVE:
		case SI_RDB_UNDEF|SI_RDB_DBSEAL|SI_RDB_REMOVE:
		case SI_RDB|SI_RDB_DBI|SI_RDB_DBSEAL:
		case SI_RDB|SI_RDB_DBI:
		case SI_RDB:
		case SI_RDB|SI_RDB_DBSEAL:
		case SI_RDB_UNDEF|SI_RDB_DBSEAL: {
			/* match and remove any leftover ancestor with
			 * its branches */
			sinode *ancestor = si_trackget(track, n->id.parent);
			if (ancestor && (ancestor != n))
				ancestor->recover |= SI_RDB_REMOVE;
			int rc = si_branchsort(r, buf, n);
			if (srunlikely(rc == -1))
				return -1;
			break;
		}
		case SI_RDB_DBSEAL: {
			/* find parent */
			sinode *parent = si_trackget(track, n->id.parent);
			if (parent) {
				/* schedule node for removal, if has incomplete merges */
				if (parent->recover & SI_RDB_DBI)
					n->recover |= SI_RDB_REMOVE;
				else
					parent->recover |= SI_RDB_REMOVE;
			}
			if (! (n->recover & SI_RDB_REMOVE)) {
				/* complete node */
				int rc = si_nodecomplete(n, r, i->conf);
				if (srunlikely(rc == -1))
					return -1;
				n->recover = SI_RDB;
			}
			break;
		}
		default:
			/* corrupted states */
			return sr_error(r->e, "corrupted database repository: %s",
			                i->conf->path);
		}
		p = sr_rbprev(&track->i, p);
	}
	return 0;
}

static inline int
si_recovercomplete(sitrack *track, sr *r, si *index, srbuf *buf)
{
	/* prepare and build primary index */
	sr_bufreset(buf);
	srrbnode *p = sr_rbmin(&track->i);
	while (p) {
		sinode *n = srcast(p, sinode, node);
		int rc = sr_bufadd(buf, r->a, &n, sizeof(sinode**));
		if (srunlikely(rc == -1))
			return sr_error(r->e, "%s", "memory allocation failed");
		p = sr_rbnext(&track->i, p);
	}
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, r);
	sr_iteropen(&i, buf, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sinode *n = sr_iterof(&i);
		if (n->recover & SI_RDB_REMOVE) {
			int rc = si_nodegc(n, r);
			if (srunlikely(rc == -1))
				return -1;
			continue;
		}
		n->recover = SI_RDB;
		si_insert(index, r, n);
		si_plannerupdate(&index->p, SI_COMPACT, n);
	}
	return 0;
}

static inline int
si_recoverindex(si *i, sr *r)
{
	sitrack track;
	si_trackinit(&track);
	srbuf buf;
	sr_bufinit(&buf);
	int rc = si_trackdir(&track, r, i);
	if (srunlikely(rc == -1))
		goto error;
	if (srunlikely(track.count == 0))
		goto error;
	rc = si_trackvalidate(&track, &buf, r, i);
	if (srunlikely(rc == -1))
		goto error;
	rc = si_recovercomplete(&track, r, i, &buf);
	if (srunlikely(rc == -1))
		goto error;
	/* set actual metrics */
	r->seq->nsn = track.nsn + 1;
	r->seq->lsn = track.lsn + 1;
	sr_buffree(&buf, r->a);
	return 0;
error:
	sr_buffree(&buf, r->a);
	si_trackfree(&track, r);
	return -1;
}

int si_recover(si *i, sr *r)
{
	int exists = sr_fileexists(i->conf->path);
	if (exists == 0)
		return si_deploy(i, r);
	return si_recoverindex(i, r);
}
