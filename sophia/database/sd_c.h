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
typedef struct sdc sdc;

struct sdcbuf {
	ssbuf  a; /* decompression */
	ssbuf  b; /* transformation */
	ssiter index_iter;
	ssiter page_iter;
};

struct sdc {
	sdio io;
	sdbuild build;
	sdbuildindex build_index;
	svupsert upsert;
	ssbuf  a; /* result */
	ssbuf  b; /* redistribute buffer */
	ssbuf  c; /* file buffer */
	ssbuf  d; /* page read buffer */
	sdcbuf e; /* compression buffer list */
};

static inline void
sd_cinit(sdc *sc)
{
	sd_ioinit(&sc->io);
	sv_upsertinit(&sc->upsert);
	sd_buildinit(&sc->build);
	sd_buildindex_init(&sc->build_index);
	ss_bufinit(&sc->a);
	ss_bufinit(&sc->b);
	ss_bufinit(&sc->c);
	ss_bufinit(&sc->d);
	ss_bufinit(&sc->e.a);
	ss_bufinit(&sc->e.b);
	memset(&sc->e.index_iter, 0, sizeof(sc->e.index_iter));
	memset(&sc->e.page_iter, 0, sizeof(sc->e.page_iter));
}

static inline void
sd_cfree(sdc *sc, sr *r)
{
	sd_iofree(&sc->io, r);
	sd_buildfree(&sc->build, r);
	sd_buildindex_free(&sc->build_index, r);
	sv_upsertfree(&sc->upsert, r);
	ss_buffree(&sc->a, r->a);
	ss_buffree(&sc->b, r->a);
	ss_buffree(&sc->c, r->a);
	ss_buffree(&sc->d, r->a);
	ss_buffree(&sc->e.a, r->a);
	ss_buffree(&sc->e.b, r->a);
}

static inline void
sd_cgc(sdc *sc, sr *r, int wm)
{
	sd_buildgc(&sc->build, r, wm);
	sd_buildindex_gc(&sc->build_index, r, wm);
	sv_upsertgc(&sc->upsert, r, 600, 512);
	ss_bufgc(&sc->a, r->a, wm);
	ss_bufgc(&sc->b, r->a, wm);
	ss_bufgc(&sc->c, r->a, wm);
	ss_bufgc(&sc->d, r->a, wm);
	ss_bufgc(&sc->e.a, r->a, wm);
	ss_bufgc(&sc->e.b, r->a, wm);
}

static inline void
sd_creset(sdc *sc, sr *r ssunused)
{
	sd_ioreset(&sc->io);
	sd_buildreset(&sc->build);
	sd_buildindex_reset(&sc->build_index);
	sv_upsertreset(&sc->upsert);
	ss_bufreset(&sc->a);
	ss_bufreset(&sc->b);
	ss_bufreset(&sc->c);
	ss_bufreset(&sc->d);
	ss_bufreset(&sc->e.a);
	ss_bufreset(&sc->e.b);
}

#endif
