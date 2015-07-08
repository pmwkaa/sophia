#ifndef ST_R_H_
#define ST_R_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct str str;

struct str {
	stconf      *conf;
	int          verbose;

	/* generator */
	int          key_start;
	int          key_end;
	int          value_start;
	int          value_end;
	stgenerator  g;
	stlist       gc;

	/* runtime */
	ssa          a;
	ssinjection  injection;
	srscheme     scheme;
	srerror      error;
	srseq        seq;
	sf           fmt;
	sfstorage    fmt_storage;
	sscrcf       crc;
	ssfilterif  *compression;
	sr           r;

	/* test runner */
	stsuite      suite;

	/* current */
	void        *env;
	void        *db;
	int          phase_scene;
	int          phase;
	sttest      *test;
	stgroup     *group;
	stplan      *plan;

	/* stats */
	int          stat_stmt;
	int          stat_test;
};

extern str st_r;

#endif
