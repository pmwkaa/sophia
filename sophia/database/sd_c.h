#ifndef SD_C_H_
#define SD_C_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdcbuf sdcbuf;
typedef struct sdcgc sdcgc;
typedef struct sdc sdc;

struct sdcbuf {
	ssbuf a; /* decompression */
	ssbuf b; /* transformation */
	ssiter index_iter;
	ssiter page_iter;
	sdcbuf *next;
};

struct sdc {
	sddirectio direct_io;
	sdbuild build;
	ssqf qf;
	svupsert upsert;
	ssbuf a;        /* result */
	ssbuf b;        /* redistribute buffer */
	ssbuf c;        /* file buffer */
	ssbuf d;        /* page read buffer */
	sdcbuf *head;   /* compression buffer list */
	int count;
};

static inline void
sd_cinit(sdc *sc)
{
	sd_directio_init(&sc->direct_io);
	sv_upsertinit(&sc->upsert);
	sd_buildinit(&sc->build);
	ss_qfinit(&sc->qf);
	ss_bufinit(&sc->a);
	ss_bufinit(&sc->b);
	ss_bufinit(&sc->c);
	ss_bufinit(&sc->d);
	sc->count = 0;
	sc->head = NULL;
}

static inline void
sd_cfree(sdc *sc, sr *r)
{
	sd_directio_free(&sc->direct_io, r);
	sd_buildfree(&sc->build, r);
	ss_qffree(&sc->qf, r->a);
	sv_upsertfree(&sc->upsert, r);
	ss_buffree(&sc->a, r->a);
	ss_buffree(&sc->b, r->a);
	ss_buffree(&sc->c, r->a);
	ss_buffree(&sc->d, r->a);
	sdcbuf *b = sc->head;
	sdcbuf *next;
	while (b) {
		next = b->next;
		ss_buffree(&b->a, r->a);
		ss_buffree(&b->b, r->a);
		ss_free(r->a, b);
		b = next;
	}
}

static inline void
sd_cgc(sdc *sc, sr *r, int wm)
{
	sd_buildgc(&sc->build, r, wm);
	ss_qfgc(&sc->qf, r->a, wm);
	sv_upsertgc(&sc->upsert, r, 600, 512);
	ss_bufgc(&sc->a, r->a, wm);
	ss_bufgc(&sc->b, r->a, wm);
	ss_bufgc(&sc->c, r->a, wm);
	ss_bufgc(&sc->d, r->a, wm);
	sdcbuf *b = sc->head;
	while (b) {
		ss_bufgc(&b->a, r->a, wm);
		ss_bufgc(&b->b, r->a, wm);
		b = b->next;
	}
}

static inline void
sd_creset(sdc *sc, sr *r ssunused)
{
	sd_directio_reset(&sc->direct_io);
	sd_buildreset(&sc->build);
	ss_qfreset(&sc->qf);
	sv_upsertreset(&sc->upsert);
	ss_bufreset(&sc->a);
	ss_bufreset(&sc->b);
	ss_bufreset(&sc->c);
	ss_bufreset(&sc->d);
	sdcbuf *b = sc->head;
	while (b) {
		ss_bufreset(&b->a);
		ss_bufreset(&b->b);
		b = b->next;
	}
}

static inline int
sd_censure(sdc *c, sr *r, int count)
{
	if (c->count < count) {
		while (count-- >= 0) {
			sdcbuf *b = ss_malloc(r->a, sizeof(sdcbuf));
			if (ssunlikely(b == NULL))
				return -1;
			ss_bufinit(&b->a);
			ss_bufinit(&b->b);
			b->next = c->head;
			c->head = b;
			c->count++;
		}
	}
	return 0;
}

#endif
