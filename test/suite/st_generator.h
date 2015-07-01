#ifndef ST_GENERATOR_H_
#define ST_GENERATOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct stgenerator stgenerator;

struct stgenerator {
	int key_start;
	int key_end;
	int value_start;
	int value_end;
	int is_random;
	uint64_t seq;
	sr *r;
};

void  st_generator_init(stgenerator*, sr*, int, int, int, int, int);
svv  *st_gsvv(stgenerator*, stlist*, uint64_t, uint8_t);
svv  *st_svv(stgenerator*, stlist*, uint64_t, uint8_t, ...);
sv   *st_gsv(stgenerator*, stlist*, uint64_t, uint8_t);
sv   *st_sv(stgenerator*, stlist*, uint64_t, uint8_t, ...);

#endif
