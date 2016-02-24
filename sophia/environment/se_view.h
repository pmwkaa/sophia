#ifndef SE_VIEW_H_
#define SE_VIEW_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct seview seview;

struct seview {
	so        o;
	uint64_t  vlsn;
	ssbuf     name;
	sx        t;
	svlog     log;
	int       db_view_only;
	solist    cursor;
} sspacked;

so  *se_viewnew(se*, uint64_t, char*);
int  se_viewupdate(seview*);

#endif
