#ifndef SE_O_H_
#define SE_O_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

enum {
	SEUNDEF,
	SE,
	SEASYNC,
	SEMETA,
	SEMETACURSOR,
	SEMETAV,
	SEREQUEST,
	SEV,
	SEDB,
	SEDBASYNC,
	SETX,
	SECURSOR,
	SESNAPSHOT
};

extern sotype se_o[];

#define se_cast(ptr, cast, id) \
	so_cast(ptr, cast, &se_o[id])

static inline so*
se_cast_validate(void *ptr)
{
	so *o = ptr;
	if ((char*)o->type >= (char*)&se_o[0] &&
	    (char*)o->type <= (char*)&se_o[SESNAPSHOT])
		return ptr;
	return NULL;
}

#endif
