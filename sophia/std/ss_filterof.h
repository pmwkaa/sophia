#ifndef SS_FILTEROF_H_
#define SS_FILTEROF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline ssfilterif*
ss_filterof(char *name)
{
	if (strcmp(name, "none") == 0)
		return &ss_nonefilter;
	if (strcmp(name, "lz4") == 0)
		return &ss_lz4filter;
	if (strcmp(name, "zstd") == 0)
		return &ss_zstdfilter;
	return NULL;
}

#endif
