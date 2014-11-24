#ifndef SE_H_
#define SE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct se se;

struct se {
	seconf *conf;
};

int se_init(se*);
int se_open(se*, sr*, seconf*);
int se_close(se*);

#endif
