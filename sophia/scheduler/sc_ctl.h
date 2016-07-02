#ifndef SC_CTL_H_
#define SC_CTL_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sc_ctl_call(sc*, uint64_t);
int sc_ctl_branch(sc*, uint64_t, si*);
int sc_ctl_compact(sc*, uint64_t, si*);
int sc_ctl_compact_index(sc*, uint64_t, si*);
int sc_ctl_snapshot(sc*, si*);
int sc_ctl_checkpoint(sc*, si*);
int sc_ctl_expire(sc*, si*);
int sc_ctl_gc(sc*, si*);
int sc_ctl_backup(sc*);

#endif
