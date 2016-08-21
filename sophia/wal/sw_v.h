#ifndef SW_V_H_
#define SW_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct swv swv;

struct swv {
	uint32_t crc;
	uint32_t dsn;
	uint32_t size;
	uint8_t  flags;
} sspacked;

static inline char*
sw_vpointer(swv *v) {
	return (char*)v + sizeof(*v);
}

#endif
