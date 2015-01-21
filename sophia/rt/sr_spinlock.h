#ifndef SR_LOCK_H_
#define SR_LOCK_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#if 0
typedef pthread_spinlock_t srspinlock;

static inline void
sr_spinlockinit(srspinlock *l) {
	pthread_spin_init(l, 0);
}

static inline void
sr_spinlockfree(srspinlock *l) {
	pthread_spin_destroy(l);
}

static inline void
sr_spinlock(srspinlock *l) {
	pthread_spin_lock(l);
}

static inline void
sr_spinunlock(srspinlock *l) {
	pthread_spin_unlock(l);
}
#endif

typedef uint8_t srspinlock;

#if defined(__x86_64__) || defined(__i386) || defined(_X86_)
# define CPU_PAUSE __asm__ ("pause")
#else
# define CPU_PAUSE do { } while(0)
#endif

static inline void
sr_spinlockinit(srspinlock *l) {
	*l = 0;
}

static inline void
sr_spinlockfree(srspinlock *l) {
	*l = 0;
}

static inline void
sr_spinlock(srspinlock *l) {
	if (__sync_lock_test_and_set(l, 1) != 0) {
		unsigned int spin_count = 0U;
		for (;;) {
			CPU_PAUSE;
			if (*l == 0U && __sync_lock_test_and_set(l, 1) == 0)
				break;
			if (++spin_count > 100U)
				usleep(0);
		}
	}
}

static inline void
sr_spinunlock(srspinlock *l) {
	__sync_lock_release(l);
}

#endif
