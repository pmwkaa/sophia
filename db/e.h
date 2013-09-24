#ifndef SP_E_H_
#define SP_E_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct spe spe;

#define SPENONE 0
#define SPE     1
#define SPEOOM  2
#define SPESYS  4
#define SPEIO   8
#define SPEF    16

struct spe {
	spspinlock lock;
	int type;
	int errno_;
	char e[256];
};

static inline void
sp_einit(spe *e) {
	e->type = SPENONE;
	e->e[0] = 0;
	sp_lockinit(&e->lock);
}

static inline void
sp_efree(spe *e) {
	sp_lockfree(&e->lock);
}

static inline int
sp_eis(spe *e) {
	sp_lock(&e->lock);
	register int is = e->type != SPENONE;
	sp_unlock(&e->lock);
	return is;
}

static inline void
sp_esetfatal(spe *e) {
	sp_lock(&e->lock);
	assert(e->type != SPENONE);
	e->type |= SPEF;
	sp_unlock(&e->lock);
}

static inline int
sp_eisfatal(spe *e) {
	sp_lock(&e->lock);
	register int is = e->type != SPENONE;
	sp_unlock(&e->lock);
	return is;
}

static inline int
sp_echeck(spe *e) {
	sp_lock(&e->lock);
	register int panic = 0;
	if (spunlikely(e->type != SPENONE)) {
		panic = (e->type & SPEF) > 0;
		if (splikely(! panic)) {
			e->type = SPENONE;
			e->e[0] = 0;
		}
	}
	sp_unlock(&e->lock);
	return panic;
}

static inline void
sp_edup(spe *dst, spe *src) {
	sp_lock(&dst->lock);
	sp_lock(&src->lock);
	dst->type = src->type;
	dst->errno_ = src->errno_;
	memcpy(dst->e, src->e, sizeof(dst->e));
	sp_unlock(&src->lock);
	sp_unlock(&dst->lock);
}

void sp_vef(spe *e, int type, va_list args);

static inline int
sp_ef(spe *e, int type, ...) {
	va_list args;
	va_start(args, type);
	sp_vef(e, type, args);
	va_end(args);
	return -1;
}

#endif
