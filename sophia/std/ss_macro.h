#ifndef SS_MACRO_H_
#define SS_MACRO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 3
#  define sshot __attribute__((hot))
#else
#  define sshot
#endif

#define sspacked __attribute__((packed))
#define ssunused __attribute__((unused))
#define ssinline __attribute__((always_inline))

typedef struct ssalignu32 ssalignu32;
typedef struct ssalignu64 ssalignu64;
typedef struct ssaligni64 ssaligni64;

struct ssalignu32 {
	uint32_t __v;
} sspacked;

struct ssalignu64 {
	uint64_t __v;
} sspacked;

struct ssaligni64 {
	int64_t __v;
} sspacked;

#define sslikely(e)   __builtin_expect(!! (e), 1)
#define ssunlikely(e) __builtin_expect(!! (e), 0)

#define sscastu32(ptr) ((ssalignu32*)(ptr))->__v
#define sscastu64(ptr) ((ssalignu64*)(ptr))->__v
#define sscasti64(ptr) ((ssaligni64*)(ptr))->__v
#define sscast(ptr, t, f) \
	((t*)((char*)(ptr) - __builtin_offsetof(t, f)))

#define ss_align(align, len) \
	(((uintptr_t)(len) + ((align) - 1)) & ~((uintptr_t)((align) - 1)))

#define ss_cmp(a, b) \
	((a) == (b) ? 0 : (((a) > (b)) ? 1 : -1))

#endif
