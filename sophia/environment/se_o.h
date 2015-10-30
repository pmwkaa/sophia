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
	SEDESTROYED,
	SE,
	SEMETA,
	SEMETACURSOR,
	SEMETAV,
	SEREQ,
	SEV,
	SEDB,
	SETX,
	SECURSOR,
	SESNAPSHOT,
	SESNAPSHOTCURSOR
};

extern sotype se_o[];

#define se_cast(ptr, cast, id) \
	so_cast(ptr, cast, &se_o[id])

static inline so*
se_cast_validate(void *ptr)
{
	so *o = ptr;
	if ((char*)o->type >= (char*)&se_o[0] &&
	    (char*)o->type <= (char*)&se_o[SESNAPSHOTCURSOR])
		return ptr;
	return NULL;
}

static inline void
se_mark_destroyed(so *o)
{
	so_init(o, &se_o[SEDESTROYED], NULL, NULL, NULL);
}

#endif
