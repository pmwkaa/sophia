
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>

typedef struct {
	srhtnode node;
	char key[32];
} htnode;

sr_htsearch(srht_search, 
            ((srcast(t->i[pos], htnode, node)->node.hash == hash) &&
             (strcmp(srcast(t->i[pos], htnode, node)->key, key) == 0)));

static void
srht_test0(stc *cx srunused)
{
	sra a;
	sr_aopen(&a, &sr_stda);

	srht ht;
	t( sr_htinit(&ht, &a, 8) == 0 );

	int i = 0;
	while (i < 3431) {
		if (sr_htisfull(&ht)) {
			t( sr_htresize(&ht, &a) == 0 );
		}
		htnode *n = sr_malloc(&a, sizeof(htnode));
		int len = snprintf(n->key, sizeof(n->key), "key_%d", i);
		n->node.hash = sr_fnv(n->key, len);
		int pos = srht_search(&ht, n->node.hash, n->key, len, NULL);
		sr_htset(&ht, pos, &n->node);
		i++;
	}

	i = 0;
	while (i < 3431) {
		htnode key;
		int len = snprintf(key.key, sizeof(key.key), "key_%d", i);
		key.node.hash = sr_fnv(key.key, len);
		int pos = srht_search(&ht, key.node.hash, key.key, len, NULL);
		t( ht.i[pos] != NULL );
		t( strcmp(srcast(ht.i[pos], htnode, node)->key, key.key) == 0 );
		i++;
	}

	i = 0;
	while (i < ht.size) {
		if (ht.i[i])
			sr_free(&a, ht.i[i]);
		i++;
	}

	sr_htfree(&ht, &a);
}

stgroup *srht_group(void)
{
	stgroup *group = st_group("srht");
	st_groupadd(group, st_test("test0", srht_test0));
	return group;
}
