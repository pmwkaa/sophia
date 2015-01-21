#ifndef SV_WRITEITER_H_
#define SV_WRITEITER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

extern sriterif sv_writeiter;

int      sv_writeiter_resume(sriter*);
uint32_t sv_writeiter_total(sriter*);

#endif
