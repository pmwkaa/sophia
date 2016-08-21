#ifndef SW_ITER_H_
#define SW_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sw_iter_open(ssiter *i, sr*, ssfile*, int);
int sw_iter_error(ssiter*);
int sw_iter_continue(ssiter*);

extern ssiterif sw_iter;

#endif
