#ifndef SV_SIFT_H_
#define SV_SIFT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

extern sriterif sv_siftiter;

int      sv_siftiter_resume(sriter*);
uint32_t sv_siftiter_total(sriter*);

#endif
