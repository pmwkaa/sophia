
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libst.h>
#include <sophia.h>

static void
env_dbid_forge(stc *cx)
{
	/* based on gh-76 */

	/* X */
	void *env = sp_env();
	t( env != NULL );
	void *ctl = sp_ctl(env);

	t( sp_set(ctl, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(ctl, "scheduler.threads", "0") == 0 );
	t( sp_set(ctl, "log.rotate_sync", "0") == 0 );
	sp_set(ctl, "db", "x");
	sp_set(ctl, "db.x.sync", "0");
	void *dbx = sp_get(ctl, "db.x");
	t( dbx != NULL );
	t( sp_open(env) == 0);

	void *o = NULL;
	char key[] = "foo";
	char val[] = "bar";

	o = sp_object(dbx);
	sp_set(o, "key",   key, sizeof(key));
	sp_set(o, "value", val, sizeof(val));
	sp_set(dbx, o);
	t( sp_destroy(env) == 0 );

	/* Y */
	env = sp_env();
	t( env != NULL );
	ctl = sp_ctl(env);

	t( sp_set(ctl, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(ctl, "scheduler.threads", "0") == 0 );
	t( sp_set(ctl, "log.rotate_sync", "0") == 0 );

	/* xxx: act as db.x, without schema storage */
	sp_set(ctl, "db", "y");
	sp_set(ctl, "db.y.sync", "0");
	void *dby = sp_get(ctl, "db.y");

	t( sp_open(env) == 0 );

	o = sp_object(dby);
	sp_set(o, "key", key, sizeof(key));
	void *result = sp_get(dby, o);
	if (result) {
		char *value = sp_get(result, "value", NULL);
		printf("%s\n", value);
		sp_destroy(result);
	}

	t( sp_destroy(env) == 0 );
}

static void
env_dbid_resolve(stc *cx)
{
	/* based on gh-76 */

	/* X */
	void *env = sp_env();
	t( env != NULL );
	void *ctl = sp_ctl(env);

	t( sp_set(ctl, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(ctl, "scheduler.threads", "0") == 0 );
	t( sp_set(ctl, "log.rotate_sync", "0") == 0 );

	sp_set(ctl, "db", "x");
	sp_set(ctl, "db.x.sync", "0");
	void *dbx = sp_get(ctl, "db.x");
	t( dbx != NULL );
	t( sp_open(env) == 0);

	void *o = NULL;
	char key[] = "foo";
	char val[] = "bar";

	o = sp_object(dbx);
	sp_set(o, "key",   key, sizeof(key));
	sp_set(o, "value", val, sizeof(val));
	sp_set(dbx, o);
	t( sp_destroy(env) == 0 );

	/* Y */
	env = sp_env();
	t( env != NULL );
	ctl = sp_ctl(env);

	t( sp_set(ctl, "sophia.path", cx->suite->sophiadir) == 0 );
	t( sp_set(ctl, "scheduler.threads", "0") == 0 );
	t( sp_set(ctl, "log.rotate_sync", "0") == 0 );

	t( sp_set(ctl, "db", "x") == 0 );
	t( sp_set(ctl, "db", "y") == 0 ); /* db.id == 2 */
	sp_set(ctl, "db.x.sync", "0");
	sp_set(ctl, "db.y.sync", "0");
	void *dby = sp_get(ctl, "db.y");

	t( sp_open(env) == 0 );

	o = sp_object(dby);
	sp_set(o, "key", key, sizeof(key));
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
