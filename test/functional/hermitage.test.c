
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

/*
   hermitage: Testing transaction isolation levels.
   github.com/ept/hermitage

   Sophia uses own implementation of SSI (Serialized Snapshot Isolation).
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libst.h>

static inline void
set(void *dest, uint32_t id, uint32_t value)
{
	void *o = st_document(id, value);
	t( sp_set(dest, o) == 0 );
}

static inline void
delete(void *dest, uint32_t id)
{
	void *o = st_document(id, id);
	t( sp_delete(dest, o) == 0 );
}

static inline void
get(void *dest, uint32_t id, int value_to_check)
{
	void *o = st_document(id, id);
	o = sp_get(dest, o);
	if (o == NULL) {
		t( value_to_check == -1 );
		return;
	}
	st_document_is(o, id, value_to_check);
	sp_destroy(o);
}

static inline void*
begin(void)
{
	void *T = sp_begin(st_r.env);
	t( T != NULL );
	return T;
}

static inline void
commit(void *dest, int result)
{
	t( sp_commit(dest) == result );
	st_phase();
}

static inline void
rollback(void *dest)
{
	t( sp_destroy(dest) == 0 );
	st_phase();
}

static void
hermitage_g0(void)
{
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	set(T1, 1, 11);
	set(T2, 1, 12);
	set(T1, 2, 21);
	commit(T1, 0);
	set(T2, 2, 22);
	commit(T2, 1); /* conflict */
	get(st_r.db, 1, 11);
	get(st_r.db, 2, 21);
}

static void
hermitage_g1a(void)
{
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	set(T1, 1, 101);
	get(T2, 1, 10);
	rollback(T1);
	get(T2, 1, 10);
	commit(T2, 0);
}

static void
hermitage_g1b(void)
{
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	set(T1, 1, 101);
	get(T2, 1, 10);
	set(T1, 1, 11);
	commit(T1, 0);
	get(T2, 1, 10);
	commit(T2, 0); /* T1(1) <- T2(1), but T2 is read only */
}

static void
hermitage_g1c(void)
{
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	set(T1, 1, 11);
	set(T2, 2, 22);
	get(T1, 2, 20);
	get(T2, 1, 10);
	commit(T1, 0);
	commit(T2, 0);
}

static void
hermitage_otv(void)
{
	/* observer transaction vanishes */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	void *T3 = begin();
	set(T1, 1, 11);
	set(T1, 2, 19);
	set(T2, 1, 12);
	commit(T1, 0);
	get(T3, 1, 10); /* snapshot created on begin */
	set(T2, 2, 18);
	get(T3, 2, 20);
	commit(T2, 1);  /* rollback on conflict */
	get(T3, 2, 20); /* transaction not sees other updates */
	get(T3, 1, 10);
	commit(T3, 0);
}

static void
hermitage_pmp(void)
{
	/* predicate-many-preceders */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	/* select * from test where value = 30 */
	void *cursor = sp_cursor(st_r.env);
	void *o = sp_document(st_r.db);
	while ((o = sp_get(cursor, o))) {
		uint32_t key = *(uint32_t*)sp_getstring(o, "key", NULL);
		t( key != 30 );
	}
	sp_destroy(cursor);
	set(T2, 3, 30);
	commit(T2, 0);
	get(T1, 1, 10);
	get(T1, 2, 20);
	get(T1, 3, -1);
	commit(T1, 0);
}

static void
hermitage_pmp_write(void)
{
	/* predicate-many-preceders */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	set(T1, 1, 20);
	set(T1, 2, 30);
	get(T2, 1, 10);
	get(T2, 2, 20);
	delete(T2, 2);
	commit(T1, 0);
	get(T2, 1, 10);
	commit(T2, 1); /* conflict */
	get(st_r.db, 1, 20);
	set(st_r.db, 2, 30);
}

static void
hermitage_p4(void)
{
	/* lost update */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	get(T1, 1, 10);
	get(T2, 1, 10);
	set(T1, 1, 11);
	set(T2, 1, 11);
	commit(T1, 0);
	commit(T2, 1); /* conflict */
}

static void
hermitage_g_single(void)
{
	/* read-skew */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	get(T1, 1, 10);
	get(T2, 1, 10);
	get(T2, 2, 20);
	set(T2, 1, 12);
	set(T2, 2, 18);
	commit(T2, 0);
	get(T1, 2, 20);
	commit(T1, 0);
}

static void
hermitage_g2_item(void)
{
	/* write-skew */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	get(T1, 1, 10);
	get(T1, 2, 20);
	get(T2, 1, 10);
	get(T2, 2, 20);
	set(T1, 1, 11);
	set(T2, 1, 21);
	commit(T1, 0);
	commit(T2, 1); /* conflict */
}

static void
hermitage_g2(void)
{
	/* anti-dependency cycles */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	void *T2 = begin();
	/* select * from test where value % 3 = 0 */
	get(T1, 1, 10);
	get(T1, 2, 20);
	get(T2, 1, 10);
	get(T2, 2, 20);
	set(T1, 3, 30);
	set(T2, 4, 42);
	commit(T1, 0);
	commit(T2, 1); /* conflict */
}

static void
hermitage_g2_two_edges0(void)
{
	/* anti-dependency cycles */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	/* select * from test */
	get(T1, 1, 10);
	get(T1, 2, 20);

	void *T2 = begin();
	set(T2, 2, 25);
	commit(T2, 0);

	void *T3 = begin();
	get(T3, 1, 10);
	get(T3, 2, 25);
	commit(T3, 0);

	set(T1, 1, 0);
	commit(T1, 1);
}

static void
hermitage_g2_two_edges1(void)
{
	/* anti-dependency cycles */
	set(st_r.db, 1, 10);
	set(st_r.db, 2, 20);

	void *T1 = begin();
	/* select * from test */
	get(T1, 1, 10);
	get(T1, 2, 20);

	void *T2 = begin();
	set(T2, 2, 25);
	commit(T2, 0);

	void *T3 = begin();
	get(T3, 1, 10);
	get(T3, 2, 25);
	commit(T3, 0);

	/*set(T1, 1, 0);*/
	commit(T1, 0);
}

static void
hermitage_gh_164(void)
{
	/*
	 * This test is not part of the Hermitage tests.
	 *
	 * source: https://github.com/pmwkaa/sophia/issues/164
	*/
	set(st_r.db, 'A', 5);
	set(st_r.db, 'B', 5);

	/* t0 */
	void *T1 = begin();

	/* t1 */
	get(T1, 'A', 5);

	/* t2 */
	void *T2 = begin();

	/* t3 */
	get(T2, 'A', 5);
	get(T2, 'B', 5);

	/* t4 */
	set(T2, 'A', 6);
	set(T2, 'B', 4);

	/* t5 */
	commit(T2, 0); /* OK */

	/* t6 */
	get(T1, 'B', 5);

	/* - - - - - */

	get(T1, 'A', 5);
	commit(T1, 0); /* OK, since T1 was read-only transaction */
}

stgroup *hermitage_group(void)
{
	stgroup *group = st_group("hermitage");
	st_groupadd(group, st_test("g0", hermitage_g0));
	st_groupadd(group, st_test("g1a", hermitage_g1a));
	st_groupadd(group, st_test("g1b", hermitage_g1b));
	st_groupadd(group, st_test("g1c", hermitage_g1c));
	st_groupadd(group, st_test("otv", hermitage_otv));
	st_groupadd(group, st_test("pmp", hermitage_pmp));
	st_groupadd(group, st_test("pmp-write", hermitage_pmp_write));
	st_groupadd(group, st_test("p4", hermitage_p4));
	st_groupadd(group, st_test("g-single", hermitage_g_single));
	st_groupadd(group, st_test("g2-item", hermitage_g2_item));
	st_groupadd(group, st_test("g2", hermitage_g2));
	st_groupadd(group, st_test("g2_two_edges0", hermitage_g2_two_edges0));
	st_groupadd(group, st_test("g2_two_edges1", hermitage_g2_two_edges1));
	st_groupadd(group, st_test("gh_164", hermitage_gh_164));
	return group;
}
