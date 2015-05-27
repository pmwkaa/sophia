
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

#define SS_RBBLACK 0
#define SS_RBRED   1
#define SS_RBUNDEF 2

ssrbnode *ss_rbmin(ssrb *t)
{
	ssrbnode *n = t->root;
	if (ssunlikely(n == NULL))
		return NULL;
	while (n->l)
		n = n->l;
	return n;
}

ssrbnode *ss_rbmax(ssrb *t)
{
	ssrbnode *n = t->root;
	if (ssunlikely(n == NULL))
		return NULL;
	while (n->r)
		n = n->r;
	return n;
}

ssrbnode *ss_rbnext(ssrb *t, ssrbnode *n)
{
	if (ssunlikely(n == NULL))
		return ss_rbmin(t);
	if (n->r) {
		n = n->r;
		while (n->l)
			n = n->l;
		return n;
	}
	ssrbnode *p;
	while ((p = n->p) && p->r == n)
		n = p;
	return p;
}

ssrbnode *ss_rbprev(ssrb *t, ssrbnode *n)
{
	if (ssunlikely(n == NULL))
		return ss_rbmax(t);
	if (n->l) {
		n = n->l;
		while (n->r)
			n = n->r;
		return n;
	}
	ssrbnode *p;
	while ((p = n->p) && p->l == n)
		n = p;
	return p;
}

static inline void
ss_rbrotate_left(ssrb *t, ssrbnode *n)
{
	ssrbnode *p = n;
	ssrbnode *q = n->r;
	ssrbnode *parent = n->p;
	if (sslikely(p->p != NULL)) {
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
ss_rbrotate_right(ssrb *t, ssrbnode *n)
{
	ssrbnode *p = n;
	ssrbnode *q = n->l;
	ssrbnode *parent = n->p;
	if (sslikely(p->p != NULL)) {
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
ss_rbset_fixup(ssrb *t, ssrbnode *n)
{
	ssrbnode *p;
	while ((p = n->p) && (p->color == SS_RBRED))
	{
		ssrbnode *g = p->p;
		if (p == g->l) {
			ssrbnode *u = g->r;
			if (u && u->color == SS_RBRED) {
				g->color = SS_RBRED;
				p->color = SS_RBBLACK;
				u->color = SS_RBBLACK;
				n = g;
			} else {
				if (n == p->r) {
					ss_rbrotate_left(t, p);
					n = p;
					p = n->p;
				}
				g->color = SS_RBRED;
				p->color = SS_RBBLACK;
				ss_rbrotate_right(t, g);
			}
		} else {
			ssrbnode *u = g->l;
			if (u && u->color == SS_RBRED) {
				g->color = SS_RBRED;
				p->color = SS_RBBLACK;
				u->color = SS_RBBLACK;
				n = g;
			} else {
				if (n == p->l) {
					ss_rbrotate_right(t, p);
					n = p;
					p = n->p;
				}
				g->color = SS_RBRED;
				p->color = SS_RBBLACK;
				ss_rbrotate_left(t, g);
			}
		}
	}
	t->root->color = SS_RBBLACK;
}

void ss_rbset(ssrb *t, ssrbnode *p, int prel, ssrbnode *n)
{
	n->color = SS_RBRED;
	n->p     = p;
	n->l     = NULL;
	n->r     = NULL;
	if (sslikely(p)) {
		assert(prel != 0);
		if (prel > 0)
			p->l = n;
		else
			p->r = n;
	} else {
		t->root = n;
	}
	ss_rbset_fixup(t, n);
}

void ss_rbreplace(ssrb *t, ssrbnode *o, ssrbnode *n)
{
	ssrbnode *p = o->p;
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

void ss_rbremove(ssrb *t, ssrbnode *n)
{
	if (ssunlikely(n->color == SS_RBUNDEF))
		return;
	ssrbnode *l = n->l;
	ssrbnode *r = n->r;
	ssrbnode *x = NULL;
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
	ssrbnode *p = n->p;
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

	if (color == SS_RBRED)
		return;
	if (n && n->color == SS_RBRED) {
		n->color = SS_RBBLACK;
		return;
	}

	ssrbnode *s;
	do {
		if (ssunlikely(n == t->root))
			break;

		if (n == p->l) {
			s = p->r;
			if (s->color == SS_RBRED)
			{
				s->color = SS_RBBLACK;
				p->color = SS_RBRED;
				ss_rbrotate_left(t, p);
				s = p->r;
			}
			if ((!s->l || (s->l->color == SS_RBBLACK)) &&
			    (!s->r || (s->r->color == SS_RBBLACK)))
			{
				s->color = SS_RBRED;
				n = p;
				p = p->p;
				continue;
			}
			if ((!s->r || (s->r->color == SS_RBBLACK)))
			{
				s->l->color = SS_RBBLACK;
				s->color    = SS_RBRED;
				ss_rbrotate_right(t, s);
				s = p->r;
			}
			s->color    = p->color;
			p->color    = SS_RBBLACK;
			s->r->color = SS_RBBLACK;
			ss_rbrotate_left(t, p);
			n = t->root;
			break;
		} else {
			s = p->l;
			if (s->color == SS_RBRED)
			{
				s->color = SS_RBBLACK;
				p->color = SS_RBRED;
				ss_rbrotate_right(t, p);
				s = p->l;
			}
			if ((!s->l || (s->l->color == SS_RBBLACK)) &&
				(!s->r || (s->r->color == SS_RBBLACK)))
			{
				s->color = SS_RBRED;
				n = p;
				p = p->p;
				continue;
			}
			if ((!s->l || (s->l->color == SS_RBBLACK)))
			{
				s->r->color = SS_RBBLACK;
				s->color    = SS_RBRED;
				ss_rbrotate_left(t, s);
				s = p->l;
			}
			s->color    = p->color;
			p->color    = SS_RBBLACK;
			s->l->color = SS_RBBLACK;
			ss_rbrotate_right(t, p);
			n = t->root;
			break;
		}
	} while (n->color == SS_RBBLACK);
	if (n)
		n->color = SS_RBBLACK;
}
