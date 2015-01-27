
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsx.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libse.h>
#include <libso.h>

static inline void
sp_error_unsupported_method(soobj *o, const char *method, ...)
{
	assert(o->env != NULL);
	assert(o->env->id == SOENV);
	va_list args;
	va_start(args, method);
	so *e = (so*)o->env;
	sr_error(&e->error, "unsupported %s(%s) operation",
	         (char*)method,
	         (char*)o->i->type(o, args));
	va_end(args);
}

SP_API void*
sp_env(void)
{
	return so_new();
}

SP_API void*
sp_ctl(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->ctl == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return NULL;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	void *h = obj->i->ctl(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return h;
}

SP_API void*
sp_object(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->object == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return NULL;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	void *h = obj->i->object(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return h;
}

SP_API int
sp_open(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->open == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	int rc = obj->i->open(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return rc;
}

SP_API int
sp_destroy(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->destroy == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	va_list args;
	va_start(args, o);
	soobj *env = obj->env;
	int rc;
	if (srunlikely(env == o)) {
		rc = obj->i->destroy(o, args);
		va_end(args);
		return rc;
	}
	so_apilock(env);
	rc = obj->i->destroy(o, args);
	so_apiunlock(env);
	va_end(args);
	return rc;
}

SP_API int sp_error(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->error == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	int rc = obj->i->error(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return rc;
}

SP_API int
sp_set(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->set == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	int rc = obj->i->set(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return rc;
}

SP_API void*
sp_get(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->get == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return NULL;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	void *h = obj->i->get(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return h;
}

SP_API int
sp_delete(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->del == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	soobj *env = obj->env;
	va_list args;
	va_start(args, o);
	so_apilock(env);
	int rc = obj->i->del(o, args);
	so_apiunlock(env);
	va_end(args);
	return rc;
}

SP_API int
sp_drop(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->drop == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	soobj *env = obj->env;
	va_list args;
	va_start(args, o);
	so_apilock(env);
	int rc = obj->i->drop(o, args);
	so_apiunlock(env);
	va_end(args);
	return rc;
}

SP_API void*
sp_begin(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->begin == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return NULL;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	void *h = obj->i->begin(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return h;
}

SP_API int
sp_prepare(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->prepare == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	soobj *env = obj->env;
	va_list args;
	va_start(args, o);
	so_apilock(env);
	int rc = obj->i->prepare(o, args);
	so_apiunlock(env);
	va_end(args);
	return rc;
}

SP_API int
sp_commit(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->commit == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return -1;
	}
	soobj *env = obj->env;
	va_list args;
	va_start(args, o);
	so_apilock(env);
	int rc = obj->i->commit(o, args);
	so_apiunlock(env);
	va_end(args);
	return rc;
}

SP_API void*
sp_cursor(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->cursor == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return NULL;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	void *cursor = obj->i->cursor(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return cursor;
}

SP_API void *sp_type(void *o, ...)
{
	soobj *obj = o;
	if (srunlikely(obj->i->type == NULL)) {
		sp_error_unsupported_method(o, __FUNCTION__);
		return NULL;
	}
	va_list args;
	va_start(args, o);
	so_apilock(obj->env);
	void *h = obj->i->type(o, args);
	so_apiunlock(obj->env);
	va_end(args);
	return h;
}
