#ifndef SO_OBJ_H_
#define SO_OBJ_H_

/*
 * sophia database
 * sehia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

enum {
	SOUNDEF      = 0L,
	SOENV        = 0x06154834L,
	SOENVASYNC   = 0x38970021L,
	SOCTL        = 0x1234FFBBL,
	SOCTLCURSOR  = 0x6AB65429L,
	SOV          = 0x2FABCDE2L,
	SODB         = 0x34591111L,
	SODBCTL      = 0x59342222L,
	SODBASYNC    = 0x24242489L,
	SOTX         = 0x13491FABL,
	SOREQUEST    = 0x48991422L,
	SOCURSOR     = 0x45ABCDFAL,
	SOSNAPSHOT   = 0x71230BAFL
};

#endif
