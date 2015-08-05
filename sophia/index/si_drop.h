#ifndef SI_DROP_H_
#define SI_DROP_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int si_drop(si*);
int si_dropmark(si*);
int si_droprepository(sischeme*, sr*, int);

#endif
