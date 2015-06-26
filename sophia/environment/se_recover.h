#ifndef SE_RECOVER_H_
#define SE_RECOVER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

int se_recoverbegin(sedb*);
int se_recoverend(sedb*);
int se_recover(se*);
int se_recover_repository(se*);

#endif
