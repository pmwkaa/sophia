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

#define sscast(N, T, F) ((T*)((char*)(N) - __builtin_offsetof(T, F)))

#define sslikely(EXPR)   __builtin_expect(!! (EXPR), 1)
#define ssunlikely(EXPR) __builtin_expect(!! (EXPR), 0)

#define ss_templatecat(a, b) ss_##a##b
#define ss_template(a, b) ss_templatecat(a, b)

#define ss_cmp(a, b) ((a) == (b) ? 0 : (((a) > (b)) ? 1 : -1))

#endif
