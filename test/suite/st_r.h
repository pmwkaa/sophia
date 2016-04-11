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
	FILE        *output;
	int          verbose;
	int          report;

	/* generator */
	int          key_start;
	int          key_end;
	int          value_start;
	int          value_end;
	stgenerator  g;
	stlist       gc;

	/* runtime */
	ssa          a;
	ssa          aref;
	ssvfs        vfs;
	ssinjection  injection;
	srstat       stat;
	sfscheme     scheme;
	srstatus     status;
	srerror      error;
	srseq        seq;
	sfstorage    fmt_storage;
	sscrcf       crc;
	ssfilterif  *compression;
	sr           r;

	/* test runner */
	stsuite      suite;

	/* current */
	void        *env;
	void        *db;
	sttest      *test;
	stgroup     *group;
	stplan      *plan;

	/* current phase */
	int          phase_compaction_scene;
	int          phase_compaction;

	/* stats */
	int          stat_stmt;
	int          stat_test;
	time_t       start;
};

extern str st_r;

#endif
