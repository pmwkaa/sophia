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
typedef struct sdcbuf sdcbuf;

struct sdcbuf {
	ssbuf a; /* decompression */
	ssbuf b; /* transformation */
	sdcbuf *next;
};

struct sdc {
	sdbuild build;
	ssbuf a;        /* result */
	ssbuf b;        /* redistribute buffer */
	ssbuf c;        /* file buffer */
	sdcbuf *head;   /* compression buffer list */
	int count;
};

static inline void
sd_cinit(sdc *sc)
{
	sd_buildinit(&sc->build);
	ss_bufinit(&sc->a);
	ss_bufinit(&sc->b);
	ss_bufinit(&sc->c);
	sc->count = 0;
	sc->head = NULL;
}

static inline void
sd_cfree(sdc *sc, sr *r)
{
	sd_buildfree(&sc->build, r);
	ss_buffree(&sc->a, r->a);
	ss_buffree(&sc->b, r->a);
	ss_buffree(&sc->c, r->a);
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
sd_creset(sdc *sc)
{
	sd_buildreset(&sc->build);
	ss_bufreset(&sc->a);
	ss_bufreset(&sc->b);
	ss_bufreset(&sc->c);
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
