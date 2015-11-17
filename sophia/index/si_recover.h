#ifndef SI_RECOVER_H_
#define SI_RECOVER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

sinode *si_bootstrap(si*, uint64_t);
int si_recover(si*);

#endif
