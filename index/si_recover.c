
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
	n->id = sr_seq(r->seq, SR_NSNNEXT);
	sdindex index;
	sd_indexinit(&index);
	rc = sd_indexbegin(&index, r->a, 0);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	rc = sd_indexadd(&index, r->a, 0, 0, 0, "", 1, "", 1, 0, 0);
	if (srunlikely(rc == -1)) {
		sd_indexfree(&index, r->a);
		si_nodefree(n, r);
		return -1;
	}
	sd_indexcommit(&index, r->a);
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
	rc = si_nodecreate(n, i->conf, NULL, &index, &build);
	sd_buildfree(&build);
	if (srunlikely(rc == -1)) {
		si_nodefree(n, r);
		return -1;
	}
	rc = si_nodeseal(n, i->conf, NULL);
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
	/* id.db.inprogress */
	/* id.id.db */
	/* id.id.db.inprogress */
	char *token = name;
	ssize_t id = si_processid(&token);
	if (srunlikely(id == -1))
		return -1;
	*parent = id;
	*nsn = id;
	if (strcmp(token, ".db") == 0)
		return SI_RDB;
	else
	if (strcmp(token, ".db.inprogress") == 0)
		return SI_RDBI;
	assert(*token == '.');
	token++;
	id = si_processid(&token);
	if (srunlikely(id == -1))
		return -1;
	*nsn = id;
	if (strcmp(token, ".db") == 0)
		return SI_RDB_LEVEL;
	else
	if (strcmp(token, ".db.inprogress") == 0)
		return SI_RDB_LEVELI;
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
		uint32_t parent = 0;
		uint32_t id = 0;
		int rc = si_process(de->d_name, &id, &parent);
		sinode *node;
		switch (rc) {
		case SI_RDBI:
		case SI_RDB:
			node = si_trackget(track, id);
			if (node == NULL) {
				node = si_nodenew(r);
				if (srunlikely(node == NULL))
					goto error;
				node->id = id;
				si_trackset(track, node);
			} else {
				node->recover &= ~SI_RDB_UNDEF;
			}
			node->recover |= rc;
			break;
		case SI_RDB_LEVELI:
		case SI_RDB_LEVEL:
			node = si_trackget(track, parent);
			if (node == NULL) {
				node = si_nodenew(r);
				if (srunlikely(node == NULL))
					goto error;
				node->id = parent;
				node->recover = SI_RDB_UNDEF;
				si_trackset(track, node);
			}
			int match = 0;
			sinode *nodeparent = node;
			while (node->next) {
				if (node->id == id) {
					node->recover |= rc;
					match = 1;
					break;
				}
				node = node->next;
			}
			if (match)
				continue;
			node = si_nodenew(r);
			if (srunlikely(node == NULL))
				goto error;
			node->id = id;
			node->recover |= rc;
			node->next = nodeparent->next;
			nodeparent->next = node;
			nodeparent->lv++;
			si_tracknsn(track, node);
			break;
		case -1:
			/* skip unknown files */
			continue;
		}
	}
	closedir(dir);
	return 0;
error:
	closedir(dir);
	return -1;
}

static int
si_levelcmp(const void *p1, const void *p2)
{
	sinode *a = *(sinode**)p1;
	sinode *b = *(sinode**)p2;
	if (a->id == b->id)
		return 0;
	return (a->id > b->id) ? 1 : -1;
}

static inline int
si_levelsort(sr *r, srbuf *buf, sinode *parent)
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
	qsort(buf->s, count, sizeof(sinode*), si_levelcmp);
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
	srrbnode *p = sr_rbmin(&track->i);
	while (p) {
		sinode *n = srcast(p, sinode, node);
		sinode *parent = n;
		sinode *prev   = n;
		if (parent->recover & SI_RDB_UNDEF)
			return -1;
		int rc;
		while (n) {
			switch (n->recover) {
			case SI_RDBI|SI_RDB:
				assert(n == parent);
				/* remove inprogress file */
				rc = si_nodeunlink(n, i->conf, NULL, 1);
				if (srunlikely(rc == -1))
					return -1;
				n->recover &= ~SI_RDBI;
				break;
			case SI_RDBI:
				assert(n == parent);
				/* ensure correct */
				/* seal */
				rc = si_nodeseal(n, i->conf, NULL);
				if (srunlikely(rc == -1))
					return -1;
				n->recover &= ~SI_RDBI;
				n->recover |= SI_RDB;
				break;
			case SI_RDB:
				assert(n == parent);
				/* ok */
				break;
			case SI_RDB_LEVELI|SI_RDB_LEVEL:
				assert(n != parent);
				/* impossible */
				return -1;
			case SI_RDB_LEVELI:
				assert(n != parent);
				/* remove inprogress */
				rc = si_nodeunlink(n, i->conf, parent, 1);
				if (srunlikely(rc == -1))
					return -1;
				/* remove node from parent */
				prev->next = n->next;
				parent->lv--;
				n->recover &= ~SI_RDB_LEVELI;
				si_nodefree(n, r);
				/* xxx ENSURE OTHER > LEVELS ARE IN_PROGRESS TOO */
				n = prev;
				break;
			case SI_RDB_LEVEL:
				assert(n != parent);
				/* ok */
				break;
			}
			prev = n;
			n = n->next;
		}
		rc = si_levelsort(r, buf, parent);
		if (srunlikely(rc == -1))
			return -1;
		p = sr_rbnext(&track->i, p);
	}
	return 0;
}

static inline int
si_trackopen(sitrack *track, srbuf *buf, sr *r, si *i)
{
	sr_bufreset(buf);
	srrbnode *p = sr_rbmin(&track->i);
	while (p) {
		sinode *n = srcast(p, sinode, node);
		sinode *parent = n;
		int rc;
		while (n) {
			sinode *parentref = parent;
			if (parent == n)
				parentref = NULL;
			rc = si_nodeopen(n, r, i->conf, parentref);
			if (srunlikely(rc == -1))
				return -1;
			si_tracklsn(track, n);
			n = n->next;
		}
		rc = sr_bufadd(buf, r->a, &parent, sizeof(sinode**));
		if (srunlikely(rc == -1))
			return -1;
		p = sr_rbnext(&track->i, p);
	}
	return 0;
}

static inline void
si_recoverbuild(si *index, sr *r, srbuf *buf)
{
	sriter i;
	sr_iterinit(&i, &sr_bufiterref, r);
	sr_iteropen(&i, buf, sizeof(sinode*));
	for (; sr_iterhas(&i); sr_iternext(&i)) {
		sinode *n = sr_iterof(&i);
		si_insert(index, r, n);
		si_plan(&index->plan, SI_MERGE, n);
	}
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
	rc = si_trackopen(&track, &buf, r, i);
	if (srunlikely(rc == -1))
		goto error;
	si_recoverbuild(i, r, &buf);

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
