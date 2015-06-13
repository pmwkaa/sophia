
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

	compaction

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

sinode *si_bootstrap(si *i, sr *r, uint32_t parent)
{
	sinode *n = si_nodenew(r);
	if (srunlikely(n == NULL))
		return NULL;
	sdid id = {
		.parent = parent,
		.flags  = 0,
		.id     = sr_seq(r->seq, SR_NSNNEXT)
	};
	sdindex index;
	sd_indexinit(&index);
	int rc = sd_indexbegin(&index, r, 0, 0);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r, 0);
		return NULL;
	}
	sdbuild build;
	sd_buildinit(&build);
	rc = sd_buildbegin(&build, r,
	                   i->conf->node_page_checksum,
	                   i->conf->compression);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r);
		sd_buildfree(&build, r);
		si_nodefree(n, r, 0);
		return NULL;
	}
	sd_buildend(&build, r);
	sdpageheader *h = sd_buildheader(&build);
	rc = sd_indexadd(&index, r,
	                 sd_buildoffset(&build),
	                 h->size + sizeof(sdpageheader),
	                 h->sizeorigin + sizeof(sdpageheader),
	                 h->count,
	                 NULL,
	                 0,
	                 NULL,
                     0,
                     0, UINT64_MAX,
                     UINT64_MAX, 0);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r);
		si_nodefree(n, r, 0);
		return NULL;
	}
	sd_buildcommit(&build);
	sd_indexcommit(&index, r, &id);
	rc = si_nodecreate(n, r, i->conf, &id, &index, &build);
	sd_buildfree(&build, r);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r, 1);
		return NULL;
	}
	return n;
}

static inline int
si_deploy(si *i, sr *r, int create_directory)
{
	int rc;
	if (srlikely(create_directory)) {
		rc = sr_filemkdir(i->conf->path);
		if (srunlikely(rc == -1)) {
			sr_malfunction(r->e, "directory '%s' create error: %s",
						   i->conf->path, strerror(errno));
			return -1;
		}
	}
	sinode *n = si_bootstrap(i, r, 0);
	if (srunlikely(n == NULL))
		return -1;
	SR_INJECTION(r->i, SR_INJECTION_SI_RECOVER_0,
	             si_nodefree(n, r, 0);
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);
	rc = si_nodecomplete(n, r, i->conf);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r, 1);
		return -1;
	}
	si_insert(i, r, n);
	si_plannerupdate(&i->p, SI_COMPACT|SI_BRANCH, n);
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
		sr_malfunction(r->e, "directory '%s' open error: %s",
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
		case SI_RDB_DBI:
		case SI_RDB_DBSEAL: {
			/* find parent node and mark it as having
			 * incomplete compaction process */
			head = si_trackget(track, id_parent);
			if (srlikely(head == NULL)) {
				head = si_nodenew(r);
				if (srunlikely(head == NULL))
					goto error;
				head->self.id.id = id_parent;
				head->recover = SI_RDB_UNDEF;
				si_trackset(track, head);
			}
			head->recover |= rc;
			/* remove any incomplete file made during compaction */
			if (rc == SI_RDB_DBI) {
				sr_pathAB(&path, i->conf->path, id_parent, id, ".db.incomplete");
				rc = sr_fileunlink(path.path);
				if (srunlikely(rc == -1)) {
					sr_malfunction(r->e, "db file '%s' unlink error: %s",
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
				si_nodefree(node, r, 0);
				goto error;
			}
			si_trackset(track, node);
			si_trackmetrics(track, node);
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
			si_nodefree(node, r, 0);
			goto error;
		}
		si_trackmetrics(track, node);

		/* track node */
		head = si_trackget(track, id);
		if (srlikely(head == NULL)) {
			si_trackset(track, node);
		} else {
			/* replace a node previously created by a
			 * incomplete compaction. */
			if (! (head->recover & SI_RDB_UNDEF)) {
				sr_malfunction(r->e, "corrupted database repository: %s",
				               i->conf->path);
				goto error;
			}
			si_trackreplace(track, head, node);
			head->recover &= ~SI_RDB_UNDEF;
			node->recover |= head->recover;
			si_nodefree(head, r, 0);
		}
	}
	closedir(dir);
	return 0;
error:
	closedir(dir);
	return -1;
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
			/* match and remove any leftover ancestor */
			sinode *ancestor = si_trackget(track, n->self.id.parent);
			if (ancestor && (ancestor != n))
				ancestor->recover |= SI_RDB_REMOVE;
			break;
		}
		case SI_RDB_DBSEAL: {
			/* find parent */
			sinode *parent = si_trackget(track, n->self.id.parent);
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
			return sr_malfunction(r->e, "corrupted database repository: %s",
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
			return sr_malfunction(r->e, "%s", "memory allocation failed");
		p = sr_rbnext(&track->i, p);
	}
	sriter i;
	sr_iterinit(sr_bufiterref, &i, r);
	sr_iteropen(sr_bufiterref, &i, buf, sizeof(sinode*));
	while (sr_iterhas(sr_bufiterref, &i))
	{
		sinode *n = sr_iterof(sr_bufiterref, &i);
		if (n->recover & SI_RDB_REMOVE) {
			int rc = si_nodefree(n, r, 1);
			if (srunlikely(rc == -1))
				return -1;
			sr_iternext(sr_bufiterref, &i);
			continue;
		}
		n->recover = SI_RDB;
		si_insert(index, r, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH, n);
		sr_iternext(sr_bufiterref, &i);
	}
	return 0;
}

static inline int
si_recoverdrop(si *i, sr *r)
{
	char path[1024];
	snprintf(path, sizeof(path), "%s/drop", i->conf->path);
	if (sr_fileexists(path)) {
		sr_malfunction(r->e, "attempt to recover a dropped database: %s:",
		               i->conf->path);
		return -1;
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
	int rc = si_recoverdrop(i, r);
	if (srunlikely(rc == -1))
		return -1;
	rc = si_trackdir(&track, r, i);
	if (srunlikely(rc == -1))
		goto error;
	if (srunlikely(track.count == 0))
		return 1;
	rc = si_trackvalidate(&track, &buf, r, i);
	if (srunlikely(rc == -1))
		goto error;
	rc = si_recovercomplete(&track, r, i, &buf);
	if (srunlikely(rc == -1))
		goto error;
	/* set actual metrics */
	if (track.nsn > r->seq->nsn)
		r->seq->nsn = track.nsn;
	if (track.lsn > r->seq->lsn)
		r->seq->lsn = track.lsn;
	sr_buffree(&buf, r->a);
	return 0;
error:
	sr_buffree(&buf, r->a);
	si_trackfree(&track, r);
	return -1;
}

int si_recover(si *i, sr *r)
{
	int exist = sr_fileexists(i->conf->path);
	if (exist == 0)
		goto deploy;
	if (i->conf->path_fail_on_exists) {
		sr_error(r->e, "directory '%s' is exists.", i->conf->path);
		return -1;
	}
	int rc = si_recoverindex(i, r);
	if (srlikely(rc <= 0))
		return rc;
deploy:
	return si_deploy(i, r, !exist);
}
