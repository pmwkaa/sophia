#ifndef SD_V_H_
#define SD_V_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdv sdv;

struct sdv {
	uint64_t lsn;
	uint32_t timestamp;
	uint8_t  flags;
	uint16_t keysize;
	uint32_t keyoffset;
	uint32_t valuesize;
	uint32_t valueoffset;
} srpacked;

extern svif sd_vif;

#endif
