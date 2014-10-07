
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
#include <si_track.h>

static inline int
si_deploy(si *i, sr *r)
{
	int rc;
	if (! i->conf->dir_create)
		return -1;
	if (! i->conf->dir_write)
		return -1;
	rc = sr_filemkdir(i->conf->dir);
	if (srunlikely(rc == -1))
		return -1;
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
	rc = sd_indexbegin(&index, r->a, 0);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	rc = sd_indexadd(&index, r->a, 0, 0, 0, 0, "", 1, "", 1, 0, 0);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r->a);
		si_nodefree(n, r);
		return -1;
	}
	sd_indexcommit(&index, r->a, &id);
	sdbuild build;
	sd_buildinit(&build, r);
	rc = sd_buildbegin(&build, 0);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r->a);
		sd_buildfree(&build);
		si_nodefree(n, r);
		return -1;
	}
	sd_buildend(&build);
	sd_buildcommit(&build);
	rc = si_nodecreate(n, i->conf, &id, &index, &build);
	sd_buildfree(&build);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	rc = si_nodeseal(n, i->conf);
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
si_process(char *name, uint32_t *nsn)
{
	/* id.db */
	/* id.db.inprogress */
	char *token = name;
	ssize_t id = si_processid(&token);
	if (srunlikely(id == -1))
		return -1;
	*nsn = id;
	if (strcmp(token, ".db") == 0)
		return SI_RDB;
	else
	if (strcmp(token, ".db.inprogress") == 0)
		return SI_RDBI;
	return -1;
}

static inline int
si_trackdir(sitrack *track, sr *r, si *i)
{
	DIR *dir = opendir(i->conf->dir);
	if (srunlikely(dir == NULL))
		return -1;
	struct dirent *de;
	while ((de = readdir(dir))) {
		if (srunlikely(de->d_name[0] == '.'))
			continue;
		uint32_t id = 0;
		int rc = si_process(de->d_name, &id);
		if (srunlikely(rc == -1))
			continue; /* skip unknown file */

		/* remove any incomplete files */
		if (srunlikely(rc == SI_RDBI)) {
			rc = si_nodeunlink(i->conf, id, 1);
			if (srunlikely(rc == -1))
				goto error;
		}
		assert(rc == SI_RDB);

		/* recover node */
		sinode *node = si_nodenew(r);
		if (srunlikely(node == NULL))
			goto error;
		rc = si_nodeopen(node, r, i->conf, id);
		if (srunlikely(rc == -1)) {
			si_nodefree(node, r);
			goto error;
		}
		si_tracknsn(track, node);
		si_tracklsn(track, node);

		/* search previous definition or an older
		 * version */
		sinode *head;
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
			if (! (node->recover & SI_RDB_UNDEF))
				goto error;
			si_trackreplace(track, head, node);
			node->next = head->next;
			node->lv   = head->lv;
			head->next = NULL;
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
		return -1;
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
si_trackvalidate(sitrack *track, srbuf *buf, sr *r)
{
	sr_bufreset(buf);
	srrbnode *p = sr_rbmax(&track->i);
	while (p) {
		sinode *n = srcast(p, sinode, node);
		/* corrupted state: branch without head */
		if (n->recover & SI_RDB_UNDEF)
			return -1;
		/* match and remove any leftover ancestor with
		 * its branches */
		sinode *ancestor = si_trackget(track, n->id.parent);
		int rc;
		if (ancestor && (ancestor != n)) {
			si_trackremove(track, ancestor);
			rc = si_nodegc(ancestor, r);
			if (srunlikely(rc == -1))
				return -1;
		}
		rc = si_branchsort(r, buf, n);
		if (srunlikely(rc == -1))
			return -1;
		p = sr_rbprev(&track->i, p);
	}
	return 0;
}

static inline int
si_recoverbuild(sitrack *track, sr *r, si *index, srbuf *buf)
{
	/* prepare and build primary index */
	sr_bufreset(buf);
	srrbnode *p = sr_rbmin(&track->i);
	while (p) {
		sinode *n = srcast(p, sinode, node);
		int rc = sr_bufadd(buf, r->a, &n, sizeof(sinode**));
		if (srunlikely(rc == -1))
			return -1;
		p = sr_rbnext(&track->i, p);
	}
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, r);
	sr_iteropen(&i, buf, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sinode *n = sr_iterof(&i);
		si_insert(index, r, n);
		si_plan(&index->plan, SI_MERGE, n);
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
	rc = si_trackvalidate(&track, &buf, r);
	if (srunlikely(rc == -1))
		goto error;
	rc = si_recoverbuild(&track, r, i, &buf);
	if (srunlikely(rc == -1))
		goto error;
	/* complete node recover */
	r->seq->nsn = track.nsn + 1;
	/* expect to be completed by logpool */
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
	int exists = sr_fileexists(i->conf->dir);
	if (exists == 0)
		return si_deploy(i, r);
	return si_recoverindex(i, r);
}
