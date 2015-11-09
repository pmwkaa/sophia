
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
#include <libsd.h>
#include <libst.h>

static void
env_dbid_forge(void)
{
	/* based on gh-76 */

	/* X */
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );
	sp_setstring(env, "db", "x", 0);
	sp_setint(env, "db.x.sync", 0);
	void *dbx = sp_getobject(env, "db.x");
	t( dbx != NULL );
	t( sp_open(env) == 0);

	void *o = NULL;
	char key[] = "foo";
	char val[] = "bar";

	o = sp_document(dbx);
	sp_setstring(o, "key",   key, sizeof(key));
	sp_setstring(o, "value", val, sizeof(val));
	sp_set(dbx, o);
	t( sp_destroy(env) == 0 );

	/* Y */
	env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );

	/* xxx: act as db.x, without schema storage */
	sp_setstring(env, "db", "y", 0);
	sp_setint(env, "db.y.sync", 0);
	void *dby = sp_getobject(env, "db.y");

	t( sp_open(env) == 0 );

	o = sp_document(dby);
	sp_setstring(o, "key", key, sizeof(key));
	void *result = sp_get(dby, o);
	t( result != NULL );
	sp_destroy(result);

	t( sp_destroy(env) == 0 );
}

static void
env_dbid_resolve(void)
{
	/* based on gh-76 */

	/* X */
	void *env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );

	sp_setstring(env, "db", "x", 0);
	sp_setint(env, "db.x.sync", 0);
	void *dbx = sp_getobject(env, "db.x");
	t( dbx != NULL );
	t( sp_open(env) == 0);

	void *o = NULL;
	char key[] = "foo";
	char val[] = "bar";

	o = sp_document(dbx);
	sp_setstring(o, "key",   key, sizeof(key));
	sp_setstring(o, "value", val, sizeof(val));
	sp_set(dbx, o);
	t( sp_destroy(env) == 0 );

	/* Y */
	env = sp_env();
	t( env != NULL );

	t( sp_setstring(env, "sophia.path", st_r.conf->sophia_dir, 0) == 0 );
	t( sp_setint(env, "scheduler.threads", 0) == 0 );
	t( sp_setint(env, "log.rotate_sync", 0) == 0 );

	t( sp_setstring(env, "db", "x", 0) == 0 );
	t( sp_setstring(env, "db", "y", 0) == 0 ); /* db.id == 2 */
	sp_setint(env, "db.x.sync", 0);
	sp_setint(env, "db.y.sync", 0);
	void *dby = sp_getobject(env, "db.y");

	t( sp_open(env) == 0 );

	o = sp_document(dby);
	sp_setstring(o, "key", key, sizeof(key));
	void *result = sp_get(dby, o);
	t( result == NULL );

	t( sp_destroy(env) == 0 );
}

stgroup *env_group(void)
{
	stgroup *group = st_group("env");
	st_groupadd(group, st_test("dbid_forge", env_dbid_forge));
	st_groupadd(group, st_test("dbid_resolve", env_dbid_resolve));
	return group;
}
