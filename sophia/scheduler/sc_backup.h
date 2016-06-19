#ifndef SC_BACKUP_H_
#define SC_BACKUP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sc_backupstart(sc*);
int sc_backupbegin(sc*);
int sc_backupend(sc*, scworker*);
int sc_backupstop(sc*);

#endif
