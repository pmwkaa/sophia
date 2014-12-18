#ifndef SR_ORDER_H_
#define SR_ORDER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SR_LT,
	SR_LTE,
	SR_GT,
	SR_GTE,
	SR_EQ,
	SR_UPDATE,
	SR_ROUTE,
	SR_RANDOM,
	SR_STOP
} srorder;

static inline srorder
sr_orderof(char *order)
{
	srorder cmp = SR_STOP;
	if (strcmp(order, ">") == 0) {
		cmp = SR_GT;
	} else
	if (strcmp(order, ">=") == 0) {
		cmp = SR_GTE;
	} else
	if (strcmp(order, "<") == 0) {
		cmp = SR_LT;
	} else
	if (strcmp(order, "<=") == 0) {
		cmp = SR_LTE;
	} else
	if (strcmp(order, "random") == 0) {
		cmp = SR_RANDOM;
	}
	return cmp;
}

static inline char*
sr_ordername(srorder o)
{
	switch (o) {
	case SR_LT:     return "<";
	case SR_LTE:    return "<=";
	case SR_GT:     return ">";
	case SR_GTE:    return ">=";
	case SR_RANDOM: return "random";
	default: break;
	}
	return NULL;
}

#endif
