#ifndef SL_V_H_
#define SL_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct slv slv;

struct slv {
	uint32_t crc;
	uint32_t dsn;
	uint32_t size;
	uint8_t  flags;
} sspacked;

static inline char*
sl_vpointer(slv *v) {
	return (char*)v + sizeof(*v);
}

#endif
