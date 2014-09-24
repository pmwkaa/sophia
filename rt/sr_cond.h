#ifndef SR_COND_H_
#define SR_COND_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srcond srcond;

struct srcond {
	pthread_cond_t c;
};

static inline void
sr_condinit(srcond *c) {
	pthread_cond_init(&c->c, NULL);
}

static inline void
sr_condfree(srcond *c) {
	pthread_cond_destroy(&c->c);
}

static inline void
sr_condsignal(srcond *c) {
	pthread_cond_signal(&c->c);
}

static inline void
sr_condwait(srcond *c, srmutex *m) {
	pthread_cond_wait(&c->c, &m->m);
}

#endif
