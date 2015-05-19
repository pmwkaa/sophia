
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static int
sr_nonefilter_init(srfilter *f srunused, va_list args srunused)
{
	return 0;
}

static int
sr_nonefilter_free(srfilter *f srunused)
{
	return 0;
}

static int
sr_nonefilter_reset(srfilter *f srunused)
{
	return 0;
}

static int
sr_nonefilter_start(srfilter *f srunused, srbuf *dest srunused)
{
	return 0;
}

static int
sr_nonefilter_next(srfilter *f srunused,
                   srbuf *dest srunused,
                   char *buf srunused, int size srunused)
{
	return 0;
}

static int
sr_nonefilter_complete(srfilter *f srunused, srbuf *dest srunused)
{
	return 0;
}

srfilterif sr_nonefilter =
{
	.name     = "none",
	.init     = sr_nonefilter_init,
	.free     = sr_nonefilter_free,
	.reset    = sr_nonefilter_reset,
	.start    = sr_nonefilter_start,
	.next     = sr_nonefilter_next,
	.complete = sr_nonefilter_complete
};
