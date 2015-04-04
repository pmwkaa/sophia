#ifndef SL_ITER_H_
#define SL_ITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sl_iter_open(sriter *i, srfile*, int);
int sl_iter_error(sriter*);
int sl_iter_continue(sriter*);

extern sriterif sl_iter;

#endif
