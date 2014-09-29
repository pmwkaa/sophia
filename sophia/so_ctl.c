
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsm.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libso.h>
#include <sophia.h>

static int
so_ctldb_set(soctl *c, char *path, va_list args)
{
	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL))
		return -1;
	char *name = token;
	so *e = c->e;
	sodb *db = (sodb*)so_dbmatch(e, name);
	if (db == NULL) {
		db = (sodb*)so_dbnew(e, name);
		if (srunlikely(db == NULL))
			return -1;
		so_objindex_register(&e->db, &db->o);
	}
	return so_dbctl_set(&db->ctl, path, args);
}

static void*
so_ctldb_get(soctl *c, char *path, va_list args)
{
	char *token;
	token = strtok_r(NULL, ".", &path);
	if (srunlikely(token == NULL))
		return NULL;
	char *name = token;
	so *e = c->e;
	sodb *db = (sodb*)so_dbmatch(e, name);
	if (db == NULL)
		return NULL;
	return so_dbctl_get(&db->ctl, path, args);
}

static int
so_ctlset(soobj *obj, va_list args)
{
	soctl *c = (soctl*)obj;
	char *path = va_arg(args, char*);
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL))
		return -1;
	if (strcmp(token, "db") == 0)
		return so_ctldb_set(c, ptr, args);
	return -1;
}

static void*
so_ctlget(soobj *obj, va_list args)
{
	soctl *c = (soctl*)obj;
	char *path = va_arg(args, char*);
	char q[200];
	snprintf(q, sizeof(q), "%s", path);
	char *ptr = NULL;
	char *token;
	token = strtok_r(q, ".", &ptr);
	if (srunlikely(token == NULL))
		return NULL;
	if (strcmp(token, "db") == 0)
		return so_ctldb_get(c, ptr, args);
	return NULL;
}

static void*
so_ctltype(soobj *o srunused, va_list args srunused) {
	return "ctl";
}

static soobjif soctlif =
{
	.ctl      = NULL,
	.open     = NULL,
	.destroy  = NULL,
	.set      = so_ctlset,
	.get      = so_ctlget,
	.del      = NULL,
	.begin    = NULL,
	.commit   = NULL,
	.rollback = NULL,
	.cursor   = NULL,
	.object   = NULL,
	.type     = so_ctltype,
	.copy     = NULL
};

void so_ctlinit(soctl *c, void *e)
{
	so_objinit(&c->o, SOCTL, &soctlif);
	c->e = e;
}
