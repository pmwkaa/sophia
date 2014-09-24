#ifndef SI_QOS_H_
#define SI_QOS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int si_qosenable(si*, int);
int si_qoslimit(si*);
int si_qos(si*, int, uint64_t);

#endif
