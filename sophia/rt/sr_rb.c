
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

#define SR_RBBLACK 0
#define SR_RBRED   1
#define SR_RBUNDEF 2

srrbnode *sr_rbmin(srrb *t)
{
	srrbnode *n = t->root;
	if (srunlikely(n == NULL))
		return NULL;
	while (n->l)
		n = n->l;
	return n;
}

srrbnode *sr_rbmax(srrb *t)
{
	srrbnode *n = t->root;
	if (srunlikely(n == NULL))
		return NULL;
	while (n->r)
		n = n->r;
	return n;
}

srrbnode *sr_rbnext(srrb *t, srrbnode *n)
{
	if (srunlikely(n == NULL))
		return sr_rbmin(t);
	if (n->r) {
		n = n->r;
		while (n->l)
			n = n->l;
		return n;
	}
	srrbnode *p;
	while ((p = n->p) && p->r == n)
		n = p;
	return p;
}

srrbnode *sr_rbprev(srrb *t, srrbnode *n)
{
	if (srunlikely(n == NULL))
		return sr_rbmax(t);
	if (n->l) {
		n = n->l;
		while (n->r)
			n = n->r;
		return n;
	}
	srrbnode *p;
	while ((p = n->p) && p->l == n)
		n = p;
	return p;
}

static inline void
sr_rbrotate_left(srrb *t, srrbnode *n)
{
	srrbnode *p = n;
	srrbnode *q = n->r;
	srrbnode *parent = n->p;
	if (srlikely(p->p != NULL)) {
		if (parent->l == p)
			parent->l = q;
		else
			parent->r = q;
	} else {
		t->root = q;
	}
	q->p = parent;
	p->p = q;
	p->r = q->l;
	if (p->r)
		p->r->p = p;
	q->l = p;
}

static inline void
sr_rbrotate_right(srrb *t, srrbnode *n)
{
	srrbnode *p = n;
	srrbnode *q = n->l;
	srrbnode *parent = n->p;
	if (srlikely(p->p != NULL)) {
		if (parent->l == p)
			parent->l = q;
		else
			parent->r = q;
	} else {
		t->root = q;
	}
	q->p = parent;
	p->p = q;
	p->l = q->r;
	if (p->l)
		p->l->p = p;
	q->r = p;
}

static inline void
sr_rbset_fixup(srrb *t, srrbnode *n)
{
	srrbnode *p;
	while ((p = n->p) && (p->color == SR_RBRED))
	{
		srrbnode *g = p->p;
		if (p == g->l) {
			srrbnode *u = g->r;
			if (u && u->color == SR_RBRED) {
				g->color = SR_RBRED;
				p->color = SR_RBBLACK;
				u->color = SR_RBBLACK;
				n = g;
			} else {
				if (n == p->r) {
					sr_rbrotate_left(t, p);
					n = p;
					p = n->p;
				}
				g->color = SR_RBRED;
				p->color = SR_RBBLACK;
				sr_rbrotate_right(t, g);
			}
		} else {
			srrbnode *u = g->l;
			if (u && u->color == SR_RBRED) {
				g->color = SR_RBRED;
				p->color = SR_RBBLACK;
				u->color = SR_RBBLACK;
				n = g;
			} else {
				if (n == p->l) {
					sr_rbrotate_right(t, p);
					n = p;
					p = n->p;
				}
				g->color = SR_RBRED;
				p->color = SR_RBBLACK;
				sr_rbrotate_left(t, g);
			}
		}
	}
	t->root->color = SR_RBBLACK;
}

void sr_rbset(srrb *t, srrbnode *p, int prel, srrbnode *n)
{
	n->color = SR_RBRED;
	n->p     = p;
	n->l     = NULL;
	n->r     = NULL;
	if (srlikely(p)) {
		assert(prel != 0);
		if (prel > 0)
			p->l = n;
		else
			p->r = n;
	} else {
		t->root = n;
	}
	sr_rbset_fixup(t, n);
}

void sr_rbreplace(srrb *t, srrbnode *o, srrbnode *n)
{
	srrbnode *p = o->p;
	if (p) {
		if (p->l == o) {
			p->l = n;
		} else {
			p->r = n;
		}
	} else {
		t->root = n;
	}
	if (o->l)
		o->l->p = n;
	if (o->r)
		o->r->p = n;
	*n = *o;
}

void sr_rbremove(srrb *t, srrbnode *n)
{
	if (srunlikely(n->color == SR_RBUNDEF))
		return;
	srrbnode *l = n->l;
	srrbnode *r = n->r;
	srrbnode *x = NULL;
	if (l == NULL) {
		x = r;
	} else
	if (r == NULL) {
		x = l;
	} else {
		x = r;
		while (x->l)
			x = x->l;
	}
	srrbnode *p = n->p;
	if (p) {
		if (p->l == n) {
			p->l = x;
		} else {
			p->r = x;
		}
	} else {
		t->root = x;
	}
	uint8_t color;
	if (l && r) {
		color    = x->color;
		x->color = n->color;
		x->l     = l;
		l->p     = x;
		if (x != r) {
			p    = x->p;
			x->p = n->p;
			n    = x->r;
			p->l = n;
			x->r = r;
			r->p = x;
		} else {
			x->p = p;
			p    = x;
			n    = x->r;
		}
	} else {
		color = n->color;
		n     = x;
	}
	if (n)
		n->p = p;

	if (color == SR_RBRED)
		return;
	if (n && n->color == SR_RBRED) {
		n->color = SR_RBBLACK;
		return;
	}

	srrbnode *s;
	do {
		if (srunlikely(n == t->root))
			break;

		if (n == p->l) {
			s = p->r;
			if (s->color == SR_RBRED)
			{
				s->color = SR_RBBLACK;
				p->color = SR_RBRED;
				sr_rbrotate_left(t, p);
				s = p->r;
			}
			if ((!s->l || (s->l->color == SR_RBBLACK)) &&
			    (!s->r || (s->r->color == SR_RBBLACK)))
			{
				s->color = SR_RBRED;
				n = p;
				p = p->p;
				continue;
			}
			if ((!s->r || (s->r->color == SR_RBBLACK)))
			{
				s->l->color = SR_RBBLACK;
				s->color    = SR_RBRED;
				sr_rbrotate_right(t, s);
				s = p->r;
			}
			s->color    = p->color;
			p->color    = SR_RBBLACK;
			s->r->color = SR_RBBLACK;
			sr_rbrotate_left(t, p);
			n = t->root;
			break;
		} else {
			s = p->l;
			if (s->color == SR_RBRED)
			{
				s->color = SR_RBBLACK;
				p->color = SR_RBRED;
				sr_rbrotate_right(t, p);
				s = p->l;
			}
			if ((!s->l || (s->l->color == SR_RBBLACK)) &&
				(!s->r || (s->r->color == SR_RBBLACK)))
			{
				s->color = SR_RBRED;
				n = p;
				p = p->p;
				continue;
			}
			if ((!s->l || (s->l->color == SR_RBBLACK)))
			{
				s->r->color = SR_RBBLACK;
				s->color    = SR_RBRED;
				sr_rbrotate_left(t, s);
				s = p->l;
			}
			s->color    = p->color;
			p->color    = SR_RBBLACK;
			s->l->color = SR_RBBLACK;
			sr_rbrotate_right(t, p);
			n = t->root;
			break;
		}
	} while (n->color == SR_RBBLACK);
	if (n)
		n->color = SR_RBBLACK;
}
