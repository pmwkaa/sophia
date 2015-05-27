#ifndef SL_ITER_H_
#define SL_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sl_iter_open(ssiter *i, sr*, ssfile*, int);
int sl_iter_error(ssiter*);
int sl_iter_continue(ssiter*);

extern ssiterif sl_iter;

#endif
