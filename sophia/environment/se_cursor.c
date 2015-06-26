
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libse.h>

void se_cursorend(secursor *c)
{
	se *e = se_of(&c->o);
	uint32_t id = c->t.id;
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c->seek.v)
		sv_vfree(c->db->r.a, c->seek.v);
	if (c->v.v)
		sv_vfree(c->db->r.a, c->v.v);
	so_listdel(&c->db->cursor, &c->o);
	se_dbunbind(e, id);
	ss_free(&e->a_cursor, c);
}

static int
se_cursordestroy(so *o)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	se *e = se_of(o);
	int shutdown = se_status(&e->status) == SE_SHUTDOWN;
	/* asynchronous */
	if (!shutdown && c->async) {
		serequest *task = se_requestnew(e, SE_REQCURSORDESTROY, &c->o, &c->db->o);
		if (ssunlikely(task == NULL))
			return -1;
		se_requestadd(e, task);
		return 0;
	}
	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQCURSORDESTROY, &c->o, &c->db->o);
	se_query(&req);
	se_requestend(&req);
	se_cursorend(c);
	return 0;
}

static void*
se_cursorget(so *o, so *v ssunused)
{
	secursor *c = se_cast(o, secursor*, SECURSOR);
	se *e = se_of(o);
	/* asynchronous */
	if (c->async) {
		serequest *task = se_requestnew(e, SE_REQCURSORGET, &c->o, &c->db->o);
		if (ssunlikely(task == NULL))
			return NULL;
		serequestarg *arg = &task->arg;
		arg->order = c->order;
		arg->vlsn_generate = 0;
		arg->vlsn = c->t.vlsn;
		arg->prefix = c->prefix;
		arg->prefixsize = c->prefixsize;
		se_requestadd(e, task);
		return &task->o;
	}
	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQCURSORGET, &c->o, &c->db->o);
	serequestarg *arg = &req.arg;
	arg->order = c->order;
	arg->vlsn_generate = 0;
	arg->vlsn = c->t.vlsn;
	arg->prefix = c->prefix;
	arg->prefixsize = c->prefixsize;
	int rc = se_query(&req);
	se_dbunref(c->db, 1);
	if (rc == 1)
		se_requestresult(&req);
	return req.result;
}

static soif secursorif =
{
	.open         = NULL,
	.destroy      = se_cursordestroy,
	.error        = NULL,
	.object       = NULL,
	.asynchronous = NULL,
	.poll         = NULL,
	.drop         = NULL,
	.setobject    = NULL,
	.setstring    = NULL,
	.setint       = NULL,
	.getobject    = NULL,
	.getstring    = NULL,
	.getint       = NULL,
	.set          = NULL,
	.update       = NULL,
	.del          = NULL,
	.get          = se_cursorget,
	.begin        = NULL,
	.prepare      = NULL,
	.commit       = NULL,
	.cursor       = NULL,
};

so *se_cursornew(sedb *db, sev *o, uint64_t vlsn, int async)
{
	se *e = se_of(&db->o);
	/* validate call */
	if (ssunlikely(o->o.parent != &db->o)) {
		sr_error(&e->error, "%s", "bad object parent");
		return NULL;
	}
	/* prepare cursor */
	secursor *c = ss_malloc(&e->a_cursor, sizeof(secursor));
	if (ssunlikely(c == NULL)) {
		sr_oom(&e->error);
		goto e0;
	}
	so_init(&c->o, &se_o[SECURSOR], &secursorif, &db->o, &e->o);
	c->db         = db;
	c->async      = async;
	c->ready      = 0;
	c->order      = o->order;
	c->prefix     = NULL;
	c->prefixsize = 0;
	c->cache      = NULL;
	memset(&c->v, 0, sizeof(c->v));
	sx_init(&e->xm, &c->t);

	/* allocate cursor cache */
	c->cache = si_cachepool_pop(&e->cachepool);
	if (ssunlikely(c->cache == NULL))
		goto e0;

	/* prepare key */
	svv *seek = NULL;
	if (o->keyc > 0) {
		/* search by key */
		seek = se_dbv(db, o, 1);
		if (ssunlikely(seek == NULL))
			goto e0;
	} else
	if (o->prefix)
	{
		/* search by prefix */
		if (sr_schemeof(&db->scheme.scheme, 0)->type == SS_STRING) {
			sfv fv;
			fv.key      = o->prefix;
			fv.r.size   = o->prefixsize;
			fv.r.offset = 0;
			seek = sv_vbuild(&e->r, &fv, 1, NULL, 0);
			if (ssunlikely(seek == NULL))
				goto e0;
			void *vptr = sv_vpointer(seek);
			c->prefix = sf_key(vptr, 0);
			c->prefixsize = sf_keysize(vptr, 0);
		} else {
			sr_error(&e->error, "%s", "prefix search is only supported for a"
			         " first string-part");
			goto e0;
		}
	}
	o->o.i->destroy(&o->o);
	sv_init(&c->seek, &sv_vif, seek, NULL);

	/* asynchronous */
	if (async) {
		serequest *task = se_requestnew(e, SE_REQCURSOROPEN, &c->o, &db->o);
		if (ssunlikely(task == NULL))
			goto e1;
		serequestarg *arg = &task->arg;
		arg->order = c->order;
		arg->vlsn_generate = 0;
		arg->vlsn = vlsn;
		arg->prefix = c->prefix;
		arg->prefixsize = c->prefixsize;
		se_dbbind(e);
		so_listadd(&db->cursor, &c->o);
		se_requestadd(e, task);
		return &c->o;
	}

	/* synchronous */
	serequest req;
	se_requestinit(e, &req, SE_REQCURSOROPEN, &c->o, &db->o);
	serequestarg *arg = &req.arg;
	arg->order = c->order;
	arg->vlsn_generate = 0;
	arg->vlsn = vlsn;
	arg->prefix = c->prefix;
	arg->prefixsize = c->prefixsize;
	se_query(&req);
	se_dbunref(c->db, 1);
	if (ssunlikely(req.rc == -1))
		goto e1;
	se_dbbind(e);
	so_listadd(&db->cursor, &c->o);
	return &c->o;
e0:
	o->o.i->destroy(&o->o);
e1:
	if (c->cache)
		si_cachepool_push(c->cache);
	if (c)
		ss_free(&e->a_cursor, c);
	return NULL;
}
