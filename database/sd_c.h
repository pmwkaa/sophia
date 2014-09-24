#ifndef SD_C_H_
#define SD_C_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdc sdc;

struct sdc {
	sdbuild build;
	srbuf a; /* result */
	srbuf b; /* split index */
	srbuf c;
	srbuf d;
};

static inline void
sd_cinit(sdc *sc, sr *r)
{
	sd_buildinit(&sc->build, r);
	sr_bufinit(&sc->a);
	sr_bufinit(&sc->b);
	sr_bufinit(&sc->c);
	sr_bufinit(&sc->d);
}

static inline void
sd_cfree(sdc *sc, sr *r)
{
	sd_buildfree(&sc->build);
	sr_buffree(&sc->a, r->a);
	sr_buffree(&sc->b, r->a);
	sr_buffree(&sc->c, r->a);
	sr_buffree(&sc->d, r->a);
}

static inline void
sd_creset(sdc *sc)
{
	sd_buildreset(&sc->build);
	sr_bufreset(&sc->a);
	sr_bufreset(&sc->b);
	sr_bufreset(&sc->c);
	sr_bufreset(&sc->d);
}

#endif
