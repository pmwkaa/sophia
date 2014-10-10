
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

SP_API void*
sp_env(void)
{
	return so_new();
}

SP_API void*
sp_ctl(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->ctl == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->ctl(o, args);
	va_end(args);
	return h;
}

SP_API void*
sp_object(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->object == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->object(o, args);
	va_end(args);
	return h;
}

SP_API int
sp_open(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->open == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->open(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_destroy(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->destroy == NULL))
		return -1;
	return oif->destroy(o);
}

SP_API int sp_error(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->error == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->error(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_set(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->set == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->set(o, args);
	va_end(args);
	return rc;
}

SP_API void*
sp_get(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->get == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->get(o, args);
	va_end(args);
	return h;
}

SP_API int
sp_delete(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->del == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->del(o, args);
	va_end(args);
	return rc;
}

SP_API void*
sp_begin(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->begin == NULL))
		return NULL;
	return oif->begin(o);
}

SP_API int
sp_commit(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->commit == NULL))
		return -1;
	va_list args;
	va_start(args, o);
	int rc = oif->commit(o, args);
	va_end(args);
	return rc;
}

SP_API int
sp_rollback(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->rollback == NULL))
		return -1;
	return oif->rollback(o);
}

SP_API void*
sp_cursor(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->cursor == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *cursor = oif->cursor(o, args);
	va_end(args);
	return cursor;
}

SP_API void *sp_type(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->type == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->type(o, args);
	va_end(args);
	return h;
}

SP_API void *sp_copy(void *o, ...)
{
	soobjif *oif = ((soobj*)o)->oif;
	if (srunlikely(oif->copy == NULL))
		return NULL;
	va_list args;
	va_start(args, o);
	void *h = oif->copy(o, args);
	va_end(args);
	return h;
}
