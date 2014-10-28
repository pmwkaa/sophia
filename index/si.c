
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

int si_init(si *i, siconf *conf)
{
	si_plannerinit(&i->p);
	sr_rbinit(&i->i);
	sr_mutexinit(&i->lock);
	sr_condinit(&i->cond);
	i->qos_limit = conf->memory_limit;
	i->qos_used  = 0;
	i->qos_wait  = 0;
	i->qos_on    = 1;
	i->conf      = conf;
	return 0;
}

int si_open(si *i, sr *r)
{
	return si_recover(i, r);
}

sr_rbtruncate(si_truncate,
              si_nodefree_all(srcast(n, sinode, node), (sr*)arg))

int si_close(si *i, sr *r)
{
	int rcret = 0;
	if (i->i.root)
		si_truncate(i->i.root, r);
	i->i.root = NULL;
	sr_condfree(&i->cond);
	sr_mutexfree(&i->lock);
	return rcret;
}

sr_rbget(si_match,
         sr_compare(cmp,
                    sd_indexpage_min(sd_indexmin(&(srcast(n, sinode, node))->index)),
                    sd_indexmin(&(srcast(n, sinode, node))->index)->sizemin,
                    key, keysize))

int si_insert(si *i, sr *r, sinode *n)
{
	sdindexpage *min = sd_indexmin(&n->index);
	srrbnode *p = NULL;
	int rc = si_match(&i->i, r->cmp, sd_indexpage_min(min), min->sizemin, &p);
	assert(! (rc == 0 && p));
	sr_rbset(&i->i, p, rc, &n->node);
	i->n++;
	return 0;
}

int si_replace(si *i, sinode *o, sinode *n)
{
	sr_rbreplace(&i->i, &o->node, &n->node);
	return 0;
}
