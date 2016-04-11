
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

static inline svref*
getv(svindex *i, sr *r, uint64_t vlsn, svv *key) {
	ssrbnode *n = NULL;
	int rc = sv_indexmatch(&i->i, r->scheme, sv_vpointer(key), sv_vsize(key), &n);
	if (rc == 0 && n) {
		return sv_refvisible(sscast(n, svref, node), vlsn);
	}
	return NULL;
}

static void
sv_index_replace0(void)
{
	svindex i;
	t( sv_indexinit(&i) == 0 );

	uint32_t key = 7;
	svref *h = st_svref(&st_r.g, NULL, 0, 0, key, NULL, 0);
	svref *n = st_svref(&st_r.g, NULL, 1, 0, key, NULL, 0);

	t( sv_indexset(&i, &st_r.r, h) == 0 );
	t( sv_indexset(&i, &st_r.r, n) == 0 );

	svv *keyv = st_svv(&st_r.g, &st_r.gc, 0, 0, key, NULL, 0);

	svref *p = getv(&i, &st_r.r, 0, keyv);
	t( p == h );

	p = getv(&i, &st_r.r, 1, keyv);
	t( p == n );
	t( h->next == NULL );

	sv_indexfree(&i, &st_r.r);
}

stgroup *sv_index_group(void)
{
	stgroup *group = st_group("svindex");
	st_groupadd(group, st_test("replace0", sv_index_replace0));
	return group;
}
