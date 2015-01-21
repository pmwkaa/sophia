#ifndef SR_MUTEX_H_
#define SR_MUTEX_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srmutex srmutex;

struct srmutex {
	pthread_mutex_t m;
};

static inline void
sr_mutexinit(srmutex *m) {
	pthread_mutex_init(&m->m, NULL);
}

static inline void
sr_mutexfree(srmutex *m) {
	pthread_mutex_destroy(&m->m);
}

static inline void
sr_mutexlock(srmutex *m) {
	pthread_mutex_lock(&m->m);
}

static inline void
sr_mutexunlock(srmutex *m) {
	pthread_mutex_unlock(&m->m);
}

#endif
