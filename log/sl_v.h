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
	uint64_t lsn;
	uint32_t valuesize;
	uint8_t  flags;
	uint16_t keysize;
} srpacked;

extern svif sl_vif;

#endif
