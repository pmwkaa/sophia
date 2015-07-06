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
sv_update(sr *r, sv *a, sv *b, sv *result)
{
	assert(sv_is(b, SVUPDATE));
	int   b_flags = sv_flags(b) & SVUPDATE;
	void *b_pointer = sv_pointer(b);
	int   b_size = sv_size(b);
	int   a_flags;
	void *a_pointer;
	int   a_size;
	/* convert delete to orphan update case */
	if (sslikely(a) && !sv_is(a, SVDELETE)) {
		a_flags = sv_flags(a) & SVUPDATE;
		a_pointer = sv_pointer(a);
		a_size = sv_size(a);
	} else {
		a_flags = 0;
		a_pointer = NULL;
		a_size = 0;
	}
	void *c_pointer;
	int c_size;
	int rc = r->fmt_update->function(a_flags, a_pointer, a_size,
	                                 b_flags, b_pointer, b_size,
	                                 r->fmt_update->arg,
	                                 &c_pointer, &c_size);
	if (ssunlikely(rc == -1))
		return -1;
	svv *c = sv_vbuildraw(r->a, c_pointer, c_size);
	free(c_pointer);
	if (ssunlikely(c == NULL))
		return sr_oom(r->e);
	c->flags = sv_flags(b) & ~SVUPDATE;
	c->lsn   = sv_lsn(b);
	sv_init(result, &sv_vif, c, NULL);
	return 0;
}

#endif
