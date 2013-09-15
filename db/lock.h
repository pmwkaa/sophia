#ifndef SP_LOCK_H_
#define SP_LOCK_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef uint8_t spspinlock;

static inline void
sp_lockinit(volatile spspinlock *l) {
	*l = 0;
}

static inline void
sp_lockfree(volatile spspinlock *l) {
	*l = 0;
}

static inline void
sp_lock(volatile spspinlock *l) {
	while (__sync_lock_test_and_set(l, 1)) {}
}

static inline void
sp_unlock(volatile spspinlock *l) {
	__sync_lock_release(l);
}

#if 0
#include <pthread.h>

typedef pthread_spinlock_t spspinlock;

static inline void
sp_lockinit(volatile spspinlock *l) {
	pthread_spin_init(l, 0);
}

static inline void
sp_lockfree(volatile spspinlock *l) {
	pthread_spin_destroy(l);
}

static inline void
sp_lock(volatile spspinlock *l) {
	pthread_spin_lock(l);
}

static inline void
sp_unlock(volatile spspinlock *l) {
	pthread_spin_unlock(l);
}
#endif

#endif
