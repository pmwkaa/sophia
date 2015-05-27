#ifndef SD_RECOVER_H_
#define SD_RECOVER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sd_recover_open(ssiter*, sr*, ssfile*);
int sd_recover_complete(ssiter*);

extern ssiterif sd_recover;

#endif
