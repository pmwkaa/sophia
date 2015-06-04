#ifndef SS_COND_H_
#define SS_COND_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sscond sscond;

struct sscond {
	pthread_cond_t c;
};

static inline void
ss_condinit(sscond *c) {
	pthread_cond_init(&c->c, NULL);
}

static inline void
ss_condfree(sscond *c) {
	pthread_cond_destroy(&c->c);
}

static inline void
ss_condsignal(sscond *c) {
	pthread_cond_signal(&c->c);
}

static inline void
ss_condwait(sscond *c, ssmutex *m) {
	pthread_cond_wait(&c->c, &m->m);
}

static inline void
ss_condtimedwait(sscond *c, ssmutex *m, struct timespec *tm) {
	pthread_cond_timedwait(&c->c, &m->m, tm);
}

#endif
