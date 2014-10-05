
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

void*
so_ctlreturn(srctl *match, void *o)
{
	so *e = o;
	int size = 0;
	int type = match->type & ~SR_CTLRO;
	char integer[64];
	char *value;
	switch (type) {
	case SR_CTLINT:
		size = snprintf(integer, sizeof(integer), "%d", *(int*)match->v);
		value = integer;
		break;
	case SR_CTLU32:
		size = snprintf(integer, sizeof(integer), "%"PRIu32, *(uint32_t*)match->v);
		value = integer;
		break;
	case SR_CTLU64:
		size = snprintf(integer, sizeof(integer), "%"PRIu64, *(uint64_t*)match->v);
		value = integer;
		break;
	case SR_CTLSTRING:
		value = *(char**)match->v;
		size = strlen(value);
		break;
	default: return NULL;
	}
	size++;
	svlocal l;
	l.lsn       = 0;
	l.flags     = 0;
	l.keysize   = strlen(match->name) + 1;
	l.key       = match->name;
	l.valuesize = size;
	l.value     = value;
	sv vp;
	svinit(&vp, &sv_localif, &l, NULL);
	svv *v = sv_valloc(&e->a, &vp);
	if (srunlikely(v == NULL))
		return NULL;
	sov *result = (sov*)so_vnew(e);
	if (srunlikely(result == NULL)) {
		sv_vfree(&e->a, v);
		return NULL;
	}
	svinit(&vp, &sv_vif, v, NULL);
	return so_vput(result, &vp);
}

static int
so_ctlsophia_set(soctl *c srunused, char *path srunused, va_list args srunused)
{
	return -1;
}

static void*
so_ctlsophia_get(soctl *c, char *path, va_list args srunused)
{
	int version_major = SR_VERSION_MAJOR - '0';
	int version_minor = SR_VERSION_MINOR - '0';
	char version[16];
	char *version_ptr = version;
	snprintf(version, sizeof(version), "%d.%d",
	         version_major,
	         version_minor);
	srctl ctls[4];
	srctl *p = ctls;
	p = sr_ctladd(p, "version",       SR_CTLSTRING|SR_CTLRO, &version_ptr,   NULL);
	p = sr_ctladd(p, "version_major", SR_CTLINT|SR_CTLRO,    &version_major, NULL);
	p = sr_ctladd(p, "version_minor", SR_CTLINT|SR_CTLRO,    &version_minor, NULL);
	p = sr_ctladd(p,  NULL,           0,                     NULL,           NULL);
	srctl *match = NULL;
	int rc = sr_ctlget(&ctls[0], &path, &match);
	if (srunlikely(rc ==  1))
		return 0; /* self */
	if (srunlikely(rc == -1))
		return NULL;
	return so_ctlreturn(match, c->e);
}

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
	if (strcmp(token, "sophia") == 0)
		return so_ctlsophia_set(c, ptr, args);
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
	if (strcmp(token, "sophia") == 0)
		return so_ctlsophia_get(c, ptr, args);
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
