
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

int si_init(si *i, sr *r, srquota *q)
{
	int rc = si_plannerinit(&i->p, r->a);
	if (srunlikely(rc == -1))
		return -1;
	sr_bufinit(&i->readbuf);
	sr_rbinit(&i->i);
	sr_mutexinit(&i->lock);
	sr_condinit(&i->cond);
	i->quota       = q;
	i->conf        = NULL;
	i->update_time = 0;
	i->read_disk   = 0;
	i->read_cache  = 0;
	i->destroyed   = 0;
	return 0;
}

int si_open(si *i, sr *r, siconf *conf)
{
	i->conf = conf;
	return si_recover(i, r);
}

sr_rbtruncate(si_truncate,
              si_nodefree(srcast(n, sinode, node), (sr*)arg, 0))

int si_close(si *i, sr *r)
{
	if (i->destroyed)
		return 0;
	int rcret = 0;
	if (i->i.root)
		si_truncate(i->i.root, r);
	i->i.root = NULL;
	sr_buffree(&i->readbuf, r->a);
	si_plannerfree(&i->p, r->a);
	sr_condfree(&i->cond);
	sr_mutexfree(&i->lock);
	i->destroyed = 1;
	return rcret;
}

sr_rbget(si_match,
         sr_compare(scheme,
                    sd_indexpage_min(&(srcast(n, sinode, node))->self.index,
                                     sd_indexmin(&(srcast(n, sinode, node))->self.index)),
                    sd_indexmin(&(srcast(n, sinode, node))->self.index)->sizemin,
                    key, keysize))

int si_insert(si *i, sr *r, sinode *n)
{
	sdindexpage *min = sd_indexmin(&n->self.index);
	srrbnode *p = NULL;
	int rc = si_match(&i->i, r->scheme,
	                  sd_indexpage_min(&n->self.index, min),
	                  min->sizemin, &p);
	assert(! (rc == 0 && p));
	sr_rbset(&i->i, p, rc, &n->node);
	i->n++;
	return 0;
}

int si_remove(si *i, sinode *n)
{
	sr_rbremove(&i->i, &n->node);
	i->n--;
	return 0;
}

int si_replace(si *i, sinode *o, sinode *n)
{
	sr_rbreplace(&i->i, &o->node, &n->node);
	return 0;
}

int si_plan(si *i, siplan *plan)
{
	si_lock(i);
	int rc = si_planner(&i->p, plan);
	si_unlock(i);
	return rc;
}

int si_execute(si *i, sr *r, sdc *c, siplan *plan, uint64_t vlsn)
{
	int rc = -1;
	switch (plan->plan) {
	case SI_CHECKPOINT:
	case SI_BRANCH:
	case SI_AGE:
		rc = si_branch(i, r, c, plan, vlsn);
		break;
	case SI_GC:
	case SI_COMPACT:
		rc = si_compact(i, r, c, plan, vlsn);
		break;
	case SI_BACKUP:
		rc = si_backup(i, r, c, plan);
		break;
	case SI_SHUTDOWN:
		rc = si_close(i, r);
		break;
	case SI_DROP:
		rc = si_drop(i, r);
		break;
	}
	return rc;
}
