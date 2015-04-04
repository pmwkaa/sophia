#ifndef SD_RECOVER_H_
#define SD_RECOVER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int sd_recover_open(sriter*, srfile*);
int sd_recover_complete(sriter*);

extern sriterif sd_recover;

#endif
