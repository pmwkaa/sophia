
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

	branching
	000000001.000000003.db.inprogress  (5)

	1. remove incomplete, mark parent as having incomplete
	2. find parent, mark as having seal
	3. add
	4. recover:
		a. if parent has incomplete and seal - remove both
		b. if parent has incomplete - remove incomplete
		c. if parent has seal - remove parent, complete seal
	5. panic (auto-recover)

	see: snapshot recover
	see: scheme recover
	see: test/crash/durability.test.c
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

sinode *si_bootstrap(si *i, uint64_t parent)
{
	sr *r = &i->r;
	/* create node */
	sinode *n = si_nodenew(r);
	if (ssunlikely(n == NULL))
		return NULL;
	sdid id = {
		.parent = parent,
		.flags  = 0,
		.id     = sr_seq(r->seq, SR_NSNNEXT)
	};
	int rc;
	rc = si_nodecreate(n, r, &i->scheme, &id);
	if (ssunlikely(rc == -1))
		goto e0;
	n->branch = &n->self;
	n->branch_count++;

	/* create index with one empty page */
	sdindex index;
	sd_indexinit(&index);
	rc = sd_indexbegin(&index);
	if (ssunlikely(rc == -1))
		goto e0;

	ssqf f, *qf = NULL;
	ss_qfinit(&f);

	sdbuild build;
	sd_buildinit(&build);
	rc = sd_buildbegin(&build, r,
	                   i->scheme.node_page_checksum,
	                   i->scheme.compression_cold,
	                   i->scheme.compression_cold_if);
	if (ssunlikely(rc == -1))
		goto e1;
	sd_buildend(&build, r);
	rc = sd_indexadd(&index, r, &build, 0);
	if (ssunlikely(rc == -1))
		goto e1;

	/* write page */
	rc = sd_writepage(r, &n->file, &build);
	if (ssunlikely(rc == -1))
		goto e1;
	/* amqf */
	if (i->scheme.amqf) {
		rc = ss_qfensure(&f, r->a, 0);
		if (ssunlikely(rc == -1))
			goto e1;
		qf = &f;
	}
	rc = sd_indexcommit(&index, r, &id, qf, n->file.size);
	if (ssunlikely(rc == -1))
		goto e1;
	ss_qffree(&f, r->a);
	/* write index */
	rc = sd_writeindex(r, &n->file, &index);
	if (ssunlikely(rc == -1))
		goto e1;
	if (i->scheme.mmap) {
		rc = si_nodemap(n, r);
		if (ssunlikely(rc == -1))
			goto e1;
	}
	si_branchset(&n->self, &index);

	sd_buildfree(&build, r);
	return n;
e1:
	ss_qffree(&f, r->a);
	sd_indexfree(&index, r);
	sd_buildfree(&build, r);
e0:
	si_nodefree(n, r, 0);
	return NULL;
}

static inline int
si_deploy(si *i, sr *r, int create_directory)
{
	/* create directory */
	int rc;
	if (sslikely(create_directory)) {
		rc = ss_vfsmkdir(r->vfs, i->scheme.path, 0755);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "directory '%s' create error: %s",
			               i->scheme.path, strerror(errno));
			return -1;
		}
	}
	/* create scheme file */
	rc = si_schemedeploy(&i->scheme, r);
	if (ssunlikely(rc == -1)) {
		sr_malfunction_set(r->e);
		return -1;
	}
	/* create initial node */
	sinode *n = si_bootstrap(i, 0);
	if (ssunlikely(n == NULL))
		return -1;
	SS_INJECTION(r->i, SS_INJECTION_SI_RECOVER_0,
	             si_nodefree(n, r, 0);
	             sr_malfunction(r->e, "%s", "error injection");
	             return -1);
	rc = si_noderename_complete(n, r, &i->scheme);
	if (ssunlikely(rc == -1)) {
		si_nodefree(n, r, 1);
		return -1;
	}
	si_insert(i, n);
	si_plannerupdate(&i->p, SI_COMPACT|SI_BRANCH|SI_TEMP, n);
	i->size = si_nodesize(n);
	return 1;
}

static inline int64_t
si_processid(char **str)
{
	char *s = *str;
	size_t v = 0;
	while (*s && *s != '.') {
		if (ssunlikely(! isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	*str = s;
	return v;
}

static inline int
si_process(char *name, uint64_t *nsn, uint64_t *parent)
{
	/* id.db */
	/* id.id.db.incomplete */
	/* id.id.db.seal */
	/* id.id.db.inprogress */
	/* id.id.db.gc */
	char *token = name;
	int64_t id = si_processid(&token);
	if (ssunlikely(id == -1))
		return -1;
	*parent = id;
	*nsn = id;
	if (strcmp(token, ".db") == 0)
		return SI_RDB;
	else
	if (strcmp(token, ".db.gc") == 0)
		return SI_RDB_REMOVE;
	if (ssunlikely(*token != '.'))
		return -1;
	token++;
	id = si_processid(&token);
	if (ssunlikely(id == -1))
		return -1;
	*nsn = id;
	if (strcmp(token, ".db.incomplete") == 0)
		return SI_RDB_DBI;
	else
	if (strcmp(token, ".db.inprogress") == 0)
		return SI_RDB_DBINPR;
	else
	if (strcmp(token, ".db.seal") == 0)
		return SI_RDB_DBSEAL;
	return -1;
}

static inline int
si_trackdir(sitrack *track, sr *r, si *i)
{
	DIR *dir = opendir(i->scheme.path);
	if (ssunlikely(dir == NULL)) {
		sr_malfunction(r->e, "directory '%s' open error: %s",
		               i->scheme.path, strerror(errno));
		return -1;
	}
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (ssunlikely(de->d_name[0] == '.'))
			continue;
		uint64_t id_parent = 0;
		uint64_t id = 0;
		int rc = si_process(de->d_name, &id, &id_parent);
		if (ssunlikely(rc == -1))
			continue; /* skip unknown file */
		si_tracknsn(track, id_parent);
		si_tracknsn(track, id);

		sinode *head, *node;
		sspath path;
		switch (rc) {
		case SI_RDB_DBI:
		case SI_RDB_DBSEAL: {
			/* find parent node and mark it as having
			 * incomplete compaction process */
			head = si_trackget(track, id_parent);
			if (sslikely(head == NULL)) {
				head = si_nodenew(r);
				if (ssunlikely(head == NULL))
					goto error;
				head->self.id.id = id_parent;
				head->recover = SI_RDB_UNDEF;
				si_trackset(track, head);
			}
			head->recover |= rc;
			/* remove any incomplete file made during compaction */
			if (rc == SI_RDB_DBI) {
				ss_pathcompound(&path, i->scheme.path, id_parent, id,
				                ".db.incomplete");
				rc = ss_vfsunlink(r->vfs, path.path);
				if (ssunlikely(rc == -1)) {
					sr_malfunction(r->e, "db file '%s' unlink error: %s",
					               path.path, strerror(errno));
					goto error;
				}
				continue;
			}
			assert(rc == SI_RDB_DBSEAL);
			/* recover 'sealed' node */
			node = si_nodenew(r);
			if (ssunlikely(node == NULL))
				goto error;
			node->recover = SI_RDB_DBSEAL;
			ss_pathcompound(&path, i->scheme.path, id_parent, id,
			                ".db.seal");
			rc = si_nodeopen(node, r, &i->scheme, &path, NULL);
			if (ssunlikely(rc == -1)) {
				si_nodefree(node, r, 0);
				goto error;
			}
			si_trackset(track, node);
			si_trackmetrics(track, node);
			continue;
		}
		case SI_RDB_REMOVE:
			ss_path(&path, i->scheme.path, id, ".db.gc");
			rc = ss_vfsunlink(r->vfs, ss_pathof(&path));
			if (ssunlikely(rc == -1)) {
				sr_malfunction(r->e, "db file '%s' unlink error: %s",
				               ss_pathof(&path), strerror(errno));
				goto error;
			}
			continue;
		case SI_RDB_DBINPR:
			/* node file needs a repair */
			sr_malfunction(r->e, "corrupted database repository: %s",
			               i->scheme.path);
			goto error;
		}
		assert(rc == SI_RDB);

		head = si_trackget(track, id);
		if (head != NULL && (head->recover & SI_RDB)) {
			/* loaded by snapshot */
			continue;
		}

		/* recover node */
		node = si_nodenew(r);
		if (ssunlikely(node == NULL))
			goto error;
		node->recover = SI_RDB;
		ss_path(&path, i->scheme.path, id, ".db");
		rc = si_nodeopen(node, r, &i->scheme, &path, NULL);
		if (ssunlikely(rc == -1)) {
			si_nodefree(node, r, 0);
			goto error;
		}
		si_trackmetrics(track, node);

		/* track node */
		if (sslikely(head == NULL)) {
			si_trackset(track, node);
		} else {
			/* replace a node previously created by a
			 * incomplete compaction */
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
si_trackvalidate(sitrack *track, ssbuf *buf, sr *r, si *i)
{
	ss_bufreset(buf);
	ssrbnode *p = ss_rbmax(&track->i);
	while (p) {
		sinode *n = sscast(p, sinode, node);
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
				int rc = si_noderename_complete(n, r, &i->scheme);
				if (ssunlikely(rc == -1))
					return -1;
				n->recover = SI_RDB;
			}
			break;
		}
		default:
			/* corrupted states */
			return sr_malfunction(r->e, "corrupted database repository: %s",
			                      i->scheme.path);
		}
		p = ss_rbprev(&track->i, p);
	}
	return 0;
}

static inline int
si_recovercomplete(sitrack *track, sr *r, si *index, ssbuf *buf)
{
	/* prepare and build primary index */
	ss_bufreset(buf);
	ssrbnode *p = ss_rbmin(&track->i);
	while (p) {
		sinode *n = sscast(p, sinode, node);
		int rc = ss_bufadd(buf, r->a, &n, sizeof(sinode*));
		if (ssunlikely(rc == -1))
			return sr_oom_malfunction(r->e);
		p = ss_rbnext(&track->i, p);
	}
	ssiter i;
	ss_iterinit(ss_bufiterref, &i);
	ss_iteropen(ss_bufiterref, &i, buf, sizeof(sinode*));
	while (ss_iterhas(ss_bufiterref, &i))
	{
		sinode *n = ss_iterof(ss_bufiterref, &i);
		if (n->recover & SI_RDB_REMOVE) {
			int rc = si_nodefree(n, r, 1);
			if (ssunlikely(rc == -1))
				return -1;
			ss_iternext(ss_bufiterref, &i);
			continue;
		}
		n->recover = SI_RDB;
		si_insert(index, n);
		si_plannerupdate(&index->p, SI_COMPACT|SI_BRANCH|SI_TEMP, n);
		ss_iternext(ss_bufiterref, &i);
	}
	return 0;
}

static inline int
si_tracksnapshot(sitrack *track, sr *r, si *i, sdsnapshot *s)
{
	/* read snapshot */
	ssiter iter;
	ss_iterinit(sd_snapshotiter, &iter);
	int rc;
	rc = ss_iteropen(sd_snapshotiter, &iter, r, s);
	if (ssunlikely(rc == -1))
		return -1;
	for (; ss_iterhas(sd_snapshotiter, &iter);
	      ss_iternext(sd_snapshotiter, &iter))
	{
		sdsnapshotnode *n = ss_iterof(sd_snapshotiter, &iter);
		/* skip updated nodes */
		sspath path;
		ss_path(&path, i->scheme.path, n->id, ".db");
		rc = ss_vfsexists(r->vfs, path.path);
		if (! rc)
			continue;
		uint64_t size = ss_vfssize(r->vfs, path.path);
		if (size != n->size_file)
			continue;
		/* recover node */
		sinode *node = si_nodenew(r);
		if (ssunlikely(node == NULL))
			return -1;
		node->recover = SI_RDB;
		rc = si_nodeopen(node, r, &i->scheme, &path, n);
		if (ssunlikely(rc == -1)) {
			si_nodefree(node, r, 0);
			return -1;
		}
		si_trackmetrics(track, node);
		si_trackset(track, node);
	}
	/* recover index temperature (read stats) */
	sdsnapshotheader *h = sd_snapshot_header(s);
	i->read_cache = h->read_cache;
	i->read_disk  = h->read_disk;
	return 0;
}

static inline void
si_recoversize(si *i)
{
	ssrbnode *pn = ss_rbmin(&i->i);
	while (pn) {
		sinode *n = sscast(pn, sinode, node);
		i->size += si_nodesize(n);
		pn = ss_rbnext(&i->i, pn);
	}
}

static inline int
si_recoverindex(si *i, sr *r, sdsnapshot *s)
{
	sitrack track;
	si_trackinit(&track);
	ssbuf buf;
	ss_bufinit(&buf);
	int rc;
	if (sd_snapshot_is(s)) {
		rc = si_tracksnapshot(&track, r, i, s);
		if (ssunlikely(rc == -1))
			goto error;
	}
	rc = si_trackdir(&track, r, i);
	if (ssunlikely(rc == -1))
		goto error;
	if (ssunlikely(track.count == 0))
		return 1;
	rc = si_trackvalidate(&track, &buf, r, i);
	if (ssunlikely(rc == -1))
		goto error;
	rc = si_recovercomplete(&track, r, i, &buf);
	if (ssunlikely(rc == -1))
		goto error;
	/* set actual metrics */
	if (track.nsn > r->seq->nsn)
		r->seq->nsn = track.nsn;
	if (track.lsn > r->seq->lsn)
		r->seq->lsn = track.lsn;
	si_recoversize(i);
	ss_buffree(&buf, r->a);
	return 0;
error:
	ss_buffree(&buf, r->a);
	si_trackfree(&track, r);
	return -1;
}

static inline int
si_recoversnapshot(si *i, sr *r, sdsnapshot *s)
{
	/* recovery stages:

	   snapshot            (1) ok
	   snapshot.incomplete (2) remove snapshot.incomplete
	   snapshot            (3) remove snapshot.incomplete, load snapshot
	   snapshot.incomplete
	*/

	/* recover snapshot file (crash recover) */
	int snapshot = 0;
	int snapshot_incomplete = 0;

	char path[1024];
	snprintf(path, sizeof(path), "%s/index", i->scheme.path);
	snapshot = ss_vfsexists(r->vfs, path);
	snprintf(path, sizeof(path), "%s/index.incomplete", i->scheme.path);
	snapshot_incomplete = ss_vfsexists(r->vfs, path);

	int rc;
	if (snapshot_incomplete) {
		rc = ss_vfsunlink(r->vfs, path);
		if (ssunlikely(rc == -1)) {
			sr_malfunction(r->e, "index file '%s' unlink error: %s",
			               path, strerror(errno));
			return -1;
		}
	}
	if (! snapshot)
		return 0;

	/* read snapshot file */
	snprintf(path, sizeof(path), "%s/index", i->scheme.path);

	ssize_t size = ss_vfssize(r->vfs, path);
	if (ssunlikely(size == -1)) {
		sr_malfunction(r->e, "index file '%s' read error: %s",
		               path, strerror(errno));
		return -1;
	}
	rc = ss_bufensure(&s->buf, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom_malfunction(r->e);
	ssfile file;
	ss_fileinit(&file, r->vfs);
	rc = ss_fileopen(&file, path);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "index file '%s' open error: %s",
		               path, strerror(errno));
		return -1;
	}
	rc = ss_filepread(&file, 0, s->buf.s, size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "index file '%s' read error: %s",
		               path, strerror(errno));
		ss_fileclose(&file);
		return -1;
	}
	ss_bufadvance(&s->buf, size);
	ss_fileclose(&file);
	return 0;
}

int si_recover(si *i)
{
	sr *r = &i->r;
	int exist = ss_vfsexists(r->vfs, i->scheme.path);
	if (exist == 0)
		goto deploy;
	int rc;
	rc = si_schemerecover(&i->scheme, r);
	if (ssunlikely(rc == -1))
		return -1;
	r->scheme = &i->scheme.scheme;
	sdsnapshot snapshot;
	sd_snapshot_init(&snapshot);
	rc = si_recoversnapshot(i, r, &snapshot);
	if (ssunlikely(rc == -1)) {
		sd_snapshot_free(&snapshot, r);
		return -1;
	}
	rc = si_recoverindex(i, r, &snapshot);
	sd_snapshot_free(&snapshot, r);
	if (sslikely(rc <= 0))
		return rc;
deploy:
	return si_deploy(i, r, !exist);
}
