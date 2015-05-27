
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

static int
ss_nonefilter_init(ssfilter *f ssunused, va_list args ssunused)
{
	return 0;
}

static int
ss_nonefilter_free(ssfilter *f ssunused)
{
	return 0;
}

static int
ss_nonefilter_reset(ssfilter *f ssunused)
{
	return 0;
}

static int
ss_nonefilter_start(ssfilter *f ssunused, ssbuf *dest ssunused)
{
	return 0;
}

static int
ss_nonefilter_next(ssfilter *f ssunused,
                   ssbuf *dest ssunused,
                   char *buf ssunused, int size ssunused)
{
	return 0;
}

static int
ss_nonefilter_complete(ssfilter *f ssunused, ssbuf *dest ssunused)
{
	return 0;
}

ssfilterif ss_nonefilter =
{
	.name     = "none",
	.init     = ss_nonefilter_init,
	.free     = ss_nonefilter_free,
	.reset    = ss_nonefilter_reset,
	.start    = ss_nonefilter_start,
	.next     = ss_nonefilter_next,
	.complete = ss_nonefilter_complete
};
