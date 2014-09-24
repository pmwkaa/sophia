#ifndef SR_E_H_
#define SR_E_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sre sre;

typedef enum {
	SRENONE,
	SRE,
	SREOOM,
	SRESYS,
	SREBADARG
} srerror;

struct sre {
	srspinlock lock;
	int fatal;
	srerror e;
	char estr[256];
	int errno_;
};

static inline void
sr_einit(sre *e) {
	e->fatal = 0;
	e->e = SRENONE;
	e->estr[0] = 0;
	e->errno_ = 0;
	sr_spinlockinit(&e->lock);
}

static inline void
sr_efree(sre *e) {
	sr_spinlockfree(&e->lock);
}

static inline int
sr_eis(sre *e) {
	sr_spinlock(&e->lock);
	register int is = e->e != SRENONE;
	sr_spinunlock(&e->lock);
	return is;
}

static inline int
sr_esetfatal(sre *e) {
	sr_spinlock(&e->lock);
	e->fatal = 1;
	sr_spinunlock(&e->lock);
	return -1;
}

static inline int
sr_eisfatal(sre *e) {
	sr_spinlock(&e->lock);
	int is = e->fatal;
	sr_spinunlock(&e->lock);
	return is;
}

static inline int
sr_ereset(sre *e) {
	int fatal = sr_eisfatal(e);
	if (fatal)
		return -1;
	sr_spinlock(&e->lock);
	e->e = SRENONE;
	e->estr[0] = 0;
	e->errno_ = 0;
	sr_spinunlock(&e->lock);
	return 0;
}

static inline int
sr_edup(sre *dst, sre *src) {
	sr_spinlock(&dst->lock);
	sr_spinlock(&src->lock);
	dst->e = src->e;
	dst->fatal = src->fatal;
	dst->errno_ = src->errno_;
	memcpy(dst->estr, src->estr, sizeof(dst->estr));
	sr_spinunlock(&src->lock);
	sr_spinunlock(&dst->lock);
	return -1;
}

void sr_ve(sre*, srerror, va_list);

static inline int
sr_e(sre *e, srerror id, ...) {
	va_list args;
	va_start(args, id);
	sr_ve(e, id, args);
	va_end(args);
	return -1;
}

static inline int
sr_efatal(sre *e, srerror id, ...) {
	va_list args;
	va_start(args, id);
	sr_ve(e, id, args);
	va_end(args);
	sr_esetfatal(e);
	return -1;
}

#endif
