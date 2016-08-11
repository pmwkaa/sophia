#ifndef SI_COMPACTION_H_
#define SI_COMPACTION_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int si_compact(si*, sdc*, siplan*, uint64_t, ssiter*, uint64_t);
int si_compact_index(si*, sdc*, siplan*, uint64_t);

#endif
