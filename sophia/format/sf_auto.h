#ifndef SF_AUTO_H_
#define SF_AUTO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline void
sf_autoset(sfscheme *s, sfv *fields, uint32_t *ts)
{
	int i;
	for (i = 0; i < s->fields_count; i++) {
		sffield *part = s->fields[i];
		if (part->timestamp == 0)
			continue;
		sfv *v = &fields[i];
		if (v->pointer)
			continue;
		assert(part->type == SS_U32);
		v->pointer = (char*)ts;
		v->size = sizeof(uint32_t);
	}
}

#endif
