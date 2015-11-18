#ifndef SD_SNAPSHOTITER_H_
#define SD_SNAPSHOTITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdsnapshotiter sdsnapshotiter;

struct sdsnapshotiter {
	sdsnapshot *s;
	sdsnapshotnode *n;
	uint32_t npos;
} sspacked;

static inline int
sd_snapshotiter_open(ssiter *i, sr *r, sdsnapshot *s)
{
	sdsnapshotiter *si = (sdsnapshotiter*)i->priv;
	si->s = s;
	si->n = NULL;
	si->npos = 0;
	if (ssunlikely(ss_bufused(&s->buf) < (int)sizeof(sdsnapshotheader)))
		goto error;
	sdsnapshotheader *h = (sdsnapshotheader*)s->buf.s;
	uint32_t crc = ss_crcs(r->crc, h, sizeof(*h), 0);
	if (h->crc != crc)
		goto error;
	if (ssunlikely((int)h->size != ss_bufused(&s->buf)))
		goto error;
	si->n = (sdsnapshotnode*)(s->buf.s + sizeof(sdsnapshotheader));
	return 0;
error:
	sr_malfunction(r->e, "%s", "snapshot file corrupted");
	return -1;
}

static inline void
sd_snapshotiter_close(ssiter *i ssunused)
{ }

static inline int
sd_snapshotiter_has(ssiter *i)
{
	sdsnapshotiter *si = (sdsnapshotiter*)i->priv;
	return si->n != NULL;
}

static inline void*
sd_snapshotiter_of(ssiter *i)
{
	sdsnapshotiter *si = (sdsnapshotiter*)i->priv;
	if (ssunlikely(si->n == NULL))
		return NULL;
	return si->n;
}

static inline void
sd_snapshotiter_next(ssiter *i)
{
	sdsnapshotiter *si = (sdsnapshotiter*)i->priv;
	if (ssunlikely(si->n == NULL))
		return;
	si->npos++;
	sdsnapshotheader *h = (sdsnapshotheader*)si->s->buf.s;
	if (si->npos < h->nodes) {
		si->n = (sdsnapshotnode*)((char*)si->n + sizeof(sdsnapshotnode) + si->n->size);
		return;
	}
	si->n = NULL;
}

extern ssiterif sd_snapshotiter;

#endif
