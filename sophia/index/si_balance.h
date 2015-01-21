#ifndef SI_BALANCE_H_
#define SI_BALANCE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int si_branch(si*, sr*, sdc*, siplan*, uint64_t);
int si_compact(si*, sr*, sdc*, siplan*, uint64_t);

#endif
