#ifndef SS_MUTEX_H_
#define SS_MUTEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssmutex ssmutex;

struct ssmutex {
	pthread_mutex_t m;
};

static inline void
ss_mutexinit(ssmutex *m) {
	pthread_mutex_init(&m->m, NULL);
}

static inline void
ss_mutexfree(ssmutex *m) {
	pthread_mutex_destroy(&m->m);
}

static inline void
ss_mutexlock(ssmutex *m) {
	pthread_mutex_lock(&m->m);
}

static inline void
ss_mutexunlock(ssmutex *m) {
	pthread_mutex_unlock(&m->m);
}

#endif
