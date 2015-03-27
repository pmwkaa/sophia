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
	srbuf buf;
	sdcbuf *next;
};

struct sdc {
	sdbuild build;
	srbuf a;        /* result */
	srbuf b;        /* redistribute buffer */
	srbuf c;        /* file buffer */
	sdcbuf *head;   /* compression buffer list */
	int count;
};

static inline void
sd_cinit(sdc *sc)
{
	sd_buildinit(&sc->build);
	sr_bufinit(&sc->a);
	sr_bufinit(&sc->b);
	sr_bufinit(&sc->c);
	sc->count = 0;
	sc->head = NULL;
}

static inline void
sd_cfree(sdc *sc, sr *r)
{
	sd_buildfree(&sc->build, r);
	sr_buffree(&sc->a, r->a);
	sr_buffree(&sc->b, r->a);
	sr_buffree(&sc->c, r->a);
	sdcbuf *b = sc->head;
	sdcbuf *next;
	while (b) {
		next = b->next;
		sr_buffree(&b->buf, r->a);
		sr_free(r->a, b);
		b = next;
	}
}

static inline void
sd_creset(sdc *sc)
{
	sd_buildreset(&sc->build);
	sr_bufreset(&sc->a);
	sr_bufreset(&sc->b);
	sr_bufreset(&sc->c);
	sdcbuf *b = sc->head;
	while (b) {
		sr_bufreset(&b->buf);
		b = b->next;
	}
}

static inline int
sd_censure(sdc *c, sr *r, int count)
{
	if (c->count < count) {
		while (count-- >= 0) {
			sdcbuf *b = sr_malloc(r->a, sizeof(sdcbuf));
			if (srunlikely(b == NULL))
				return -1;
			sr_bufinit(&b->buf);
			b->next = c->head;
			c->head = b;
			c->count++;
		}
	}
	return 0;
}

#endif
