
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
#include <libsp.h>

static inline void
sp_unsupported(so *o, const char *method)
{
	fprintf(stderr, "%s(%s): unsupported operation",
	        (char*)method, o->type->name);
	abort();
}

static inline so*
sp_cast(void *ptr, const char *method)
{
	so *o = se_cast_validate(ptr);
	if (ssunlikely(o == NULL)) {
		fprintf(stderr, "%s(x%p): bad object", method, ptr);
		abort();
	}
	return o;
}

SP_API void *sp_env(void)
{
	return se_new();
}

SP_API void *sp_object(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->object == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->object(o);
	se_apiunlock(o->env);
	return h;
}

SP_API int sp_open(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->open == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->open(o);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_drop(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->drop == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->drop(o);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_destroy(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->drop == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	so *env = o->env;
	int rc;
	if (ssunlikely(env == o)) {
		rc = o->i->destroy(o);
		return rc;
	}
	se_apilock(env);
	rc = o->i->destroy(o);
	se_apiunlock(env);
	return rc;
}

SP_API int sp_error(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->error == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->error(o);
	se_apiunlock(o->env);
	return rc;
}

SP_API void *sp_asynchronous(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->asynchronous == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->asynchronous(o);
	se_apiunlock(o->env);
	return h;
}

SP_API void *sp_poll(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->poll == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->poll(o);
	se_apiunlock(o->env);
	return h;
}

SP_API int sp_setobject(void *ptr, char *path, void *object)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->setobject == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->setobject(o, path, object);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_setstring(void *ptr, char *path, void *pointer, int size)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->setstring == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->setstring(o, path, pointer, size);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_setint(void *ptr, char *path, int64_t v)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->setint == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->setint(o, path, v);
	se_apiunlock(o->env);
	return rc;
}

SP_API void *sp_getobject(void *ptr, char *path)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->getobject == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->getobject(o, path);
	se_apiunlock(o->env);
	return h;
}

SP_API void *sp_getstring(void *ptr, char *path, int *size)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->getstring == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->getstring(o, path, size);
	se_apiunlock(o->env);
	return h;
}

SP_API int64_t sp_getint(void *ptr, char *path)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->getint == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int64_t rc = o->i->getint(o, path);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_set(void *ptr, void *v)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->set == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->set(o, v);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_update(void *ptr, void *v)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->update == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->update(o, v);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_delete(void *ptr, void *v)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->del == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->del(o, v);
	se_apiunlock(o->env);
	return rc;
}

SP_API void *sp_get(void *ptr, void *v)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->get == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->get(o, v);
	se_apiunlock(o->env);
	return h;
}

SP_API void *sp_cursor(void *ptr, void *v)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->cursor == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->cursor(o, v);
	se_apiunlock(o->env);
	return h;
}

SP_API void *sp_begin(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->begin == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return NULL;
	}
	se_apilock(o->env);
	void *h = o->i->begin(o);
	se_apiunlock(o->env);
	return h;
}

SP_API int sp_prepare(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->prepare == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->prepare(o);
	se_apiunlock(o->env);
	return rc;
}

SP_API int sp_commit(void *ptr)
{
	so *o = sp_cast(ptr, __FUNCTION__);
	if (ssunlikely(o->i->commit == NULL)) {
		sp_unsupported(o, __FUNCTION__);
		return -1;
	}
	se_apilock(o->env);
	int rc = o->i->commit(o);
	se_apiunlock(o->env);
	return rc;
}
