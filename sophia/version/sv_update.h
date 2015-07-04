#ifndef SV_UPDATE_H_
#define SV_UPDATE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline int
sv_update(sr *r, sv *src, sv *update, sv *result)
{
	assert(sv_is(update, SVUPDATE));
	void *pointer_src;
	int size_src;
	if (sslikely(src)) {
		assert(! sv_is(src, SVUPDATE));
		pointer_src = sv_pointer(src);
		size_src = sv_size(src);
	} else {
		pointer_src = NULL;
		size_src = 0;
	}
	void *pointer;
	int size;
	int rc = r->fmt_update->function(pointer_src, size_src,
	                                 sv_pointer(update), sv_size(update),
	                                 r->fmt_update->arg,
	                                 &pointer, &size);
	if (ssunlikely(rc == -1))
		return -1;
	svv *v = sv_vbuildraw(r->a, pointer, size);
	free(pointer);
	if (ssunlikely(v == NULL))
		return -1;
	v->flags = sv_flags(update) & ~SVUPDATE;
	v->lsn   = sv_lsn(update);
	sv_init(result, &sv_vif, v, NULL);
	return 0;
}

#endif
