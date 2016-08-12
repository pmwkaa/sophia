
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

si *si_init(sr *r, so *object)
{
	si *i = ss_malloc(r->a, sizeof(si));
	if (ssunlikely(i == NULL))
		return NULL;
	i->r = *r;
	int rc = si_plannerinit(&i->p, r->a, i);
	if (ssunlikely(rc == -1)) {
		ss_free(r->a, i);
		return NULL;
	}
	sd_cinit(&i->rdc);
	ss_rbinit(&i->i);
	ss_mutexinit(&i->lock);
	si_schemeinit(&i->scheme);
	ss_listinit(&i->link);
	ss_listinit(&i->gc);
	i->gc_count     = 0;
	i->update_time  = 0;
	i->size         = 0;
	i->read_disk    = 0;
	i->read_cache   = 0;
	i->backup       = 0;
	i->n            = 0;
	i->object       = object;
	return i;
}

int si_open(si *i)
{
	return si_recover(i);
}

ss_rbtruncate(si_truncate,
              si_nodefree(sscast(n, sinode, node), (sr*)arg, 0))

int si_close(si *i)
{
	int rc_ret = 0;
	int rc = 0;
	sslist *p, *n;
	ss_listforeach_safe(&i->gc, p, n) {
		sinode *node = sscast(p, sinode, gc);
		rc = si_nodefree(node, &i->r, 1);
		if (ssunlikely(rc == -1))
			rc_ret = -1;
	}
	ss_listinit(&i->gc);
	i->gc_count = 0;
	if (i->i.root)
		si_truncate(i->i.root, &i->r);
	i->i.root = NULL;
	sd_cfree(&i->rdc, &i->r);
	si_plannerfree(&i->p, i->r.a);
	ss_mutexfree(&i->lock);
	si_schemefree(&i->scheme, &i->r);
	ss_free(i->r.a, i);
	return rc_ret;
}

ss_rbget(si_match,
         sf_compare(scheme,
                    sd_indexpage_min(&(sscast(n, sinode, node))->index,
                                     sd_indexmin(&(sscast(n, sinode, node))->index)),
                    key))

int si_insert(si *i, sinode *n)
{
	sdindexpage *min = sd_indexmin(&n->index);
	ssrbnode *p = NULL;
	int rc = si_match(&i->i, i->r.scheme,
	                  sd_indexpage_min(&n->index, min),
	                  min->sizemin, &p);
	assert(! (rc == 0 && p));
	ss_rbset(&i->i, p, rc, &n->node);
	i->n++;
	return 0;
}

int si_remove(si *i, sinode *n)
{
	ss_rbremove(&i->i, &n->node);
	i->n--;
	return 0;
}

int si_replace(si *i, sinode *o, sinode *n)
{
	ss_rbreplace(&i->i, &o->node, &n->node);
	return 0;
}

siplannerrc
si_plan(si *i, siplan *plan)
{
	si_lock(i);
	siplannerrc rc = si_planner(&i->p, plan);
	si_unlock(i);
	return rc;
}

int
si_execute(si *i, sdc *c, siplan *plan, uint64_t vlsn)
{
	int rc = -1;
	switch (plan->plan) {
	case SI_COMPACT_INDEX:
	case SI_GC:
	case SI_EXPIRE:
		rc = si_compact_index(i, c, plan, vlsn);
		break;
	case SI_BACKUP:
	case SI_BACKUPEND:
		rc = si_backup(i, c, plan);
		break;
	case SI_NODEGC:
		rc = si_nodefree(plan->node, &i->r, 1);
		break;
	default:
		assert(0);
		break;
	}
	/* garbage collect buffers */
	sd_cgc(c, &i->r, i->scheme.buf_gc_wm);
	return rc;
}
