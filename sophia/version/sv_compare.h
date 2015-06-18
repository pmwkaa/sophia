#ifndef SV_COMPARE_H_
#define SV_COMPARE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline int
sv_compare(sv *a, sv *b, srscheme *s) {
	return sr_compare(s, sv_pointer(a), sv_size(a),
	                     sv_pointer(b), sv_size(b));
}

#endif
