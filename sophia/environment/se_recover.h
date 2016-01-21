#ifndef SE_RECOVER_H_
#define SE_RECOVER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

enum {
	SE_RECOVER_1P = 1,
	SE_RECOVER_2P = 2,
	SE_RECOVER_NP = 3
};

int se_recoverbegin(sedb*);
int se_recoverend(sedb*);
int se_recover(se*);
int se_recover_repository(se*);

#endif
