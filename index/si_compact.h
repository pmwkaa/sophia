#ifndef SI_COMPACT_H_
#define SI_COMPACT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int si_compact(si*, sr*, sdc*, siplan*, uint64_t);
int si_compactindex(si*, sr*, sdc*, siplan*, uint64_t);

#endif
