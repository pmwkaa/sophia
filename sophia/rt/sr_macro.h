#ifndef SR_MACRO_H_
#define SR_MACRO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#if __GNUC__ >= 4 && __GNUC_MINOR__ >= 3
#  define srhot __attribute__((hot))
#else
#  define srhot
#endif

#define srpacked __attribute__((packed))
#define srunused __attribute__((unused))
#define srinline __attribute__((always_inline))

#define srcast(N, T, F) ((T*)((char*)(N) - __builtin_offsetof(T, F)))

#define srlikely(EXPR)   __builtin_expect(!! (EXPR), 1)
#define srunlikely(EXPR) __builtin_expect(!! (EXPR), 0)

#define sr_templatecat(a, b) sr_##a##b
#define sr_template(a, b) sr_templatecat(a, b)

#define sr_cmp(a, b) ((a) == (b) ? 0 : (((a) > (b)) ? 1 : -1))

#endif
