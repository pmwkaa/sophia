#ifndef SS_ORDER_H_
#define SS_ORDER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SS_LT,
	SS_LTE,
	SS_GT,
	SS_GTE,
	SS_EQ,
	SS_HAS,
	SS_ROUTE,
	SS_STOP
} ssorder;

static inline ssorder
ss_orderof(char *order)
{
	ssorder cmp = SS_STOP;
	if (strcmp(order, ">") == 0) {
		cmp = SS_GT;
	} else
	if (strcmp(order, ">=") == 0) {
		cmp = SS_GTE;
	} else
	if (strcmp(order, "<") == 0) {
		cmp = SS_LT;
	} else
	if (strcmp(order, "<=") == 0) {
		cmp = SS_LTE;
	}
	return cmp;
}

static inline char*
ss_ordername(ssorder o)
{
	switch (o) {
	case SS_LT:  return "<";
	case SS_LTE: return "<=";
	case SS_GT:  return ">";
	case SS_GTE: return ">=";
	default: break;
	}
	return NULL;
}

#endif
