#ifndef SE_RECOVER_H_
#define SE_RECOVER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int se_recover_database(sedb*);
int se_recover(se*);
int se_recover_repository(se*);

#endif
