#ifndef SI_NODEVIEW_H_
#define SI_NODEVIEW_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sinodeview sinodeview;

struct sinodeview {
	sinode   *node;
	uint16_t  flags;
	uint32_t  branch_count;
};

static inline void
si_nodeview_init(sinodeview *v, sinode *node)
{
	v->node         = node;
	v->branch_count = node->branch_count;
	v->flags        = node->flags;
}

static inline void
si_nodeview_open(sinodeview *v, sinode *node)
{
	si_noderef(node);
	si_nodeview_init(v, node);
}

static inline void
si_nodeview_close(sinodeview *v)
{
	si_nodeunref(v->node);
	v->node = NULL;
}

#endif
