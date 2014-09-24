
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

int si_qosenable(si *i, int e) {
	i->qos_on = e;
	return 0;
}

int si_qoslimit(si *i)
{
	if (i->qos_on == 0 || i->qos_limit == 0)
		return 0;
	return i->qos_used + (2 * 1024 * 1024) >= i->qos_limit;
}

int si_qos(si *i, int op, uint64_t v)
{
	switch (op) {
	case 0:
		i->qos_used += v;
		if (!i->qos_on || i->qos_limit == 0)
			return 0;
		if (srunlikely(i->qos_used + v >= i->qos_limit)) {
			i->qos_wait++;
			sr_condwait(&i->cond, &i->lock);
		}
		break;
	case 1:
		i->qos_used -= v;
		if (!i->qos_on || i->qos_limit == 0)
			return 0;
		if (srunlikely(i->qos_used < i->qos_limit && i->qos_wait)) {
			i->qos_wait--;
			sr_condsignal(&i->cond);
		}
		break;
	}
	return 0;
}
