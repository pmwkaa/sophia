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
	uint32_t offset;
	uint32_t size;
} srpacked;

extern svif sd_vif;

#endif
