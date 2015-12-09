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

#define sslikely(EXPR) __builtin_expect(!! (EXPR), 1)
#define ssunlikely(EXPR) __builtin_expect(!! (EXPR), 0)

#define sscastu32(ptr) ((ssalignu32*)(ptr))->__v
#define sscastu64(ptr) ((ssalignu64*)(ptr))->__v
#define sscasti64(ptr) ((ssaligni64*)(ptr))->__v

#define sscast(N, T, F) ((T*)((char*)(N) - __builtin_offsetof(T, F)))

#define ss_templatecat(a, b) ss_##a##b
#define ss_template(a, b) ss_templatecat(a, b)
#define ss_cmp(a, b) ((a) == (b) ? 0 : (((a) > (b)) ? 1 : -1))

#endif
