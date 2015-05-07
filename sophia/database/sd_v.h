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
	uint32_t offset:21;
	uint8_t  flags:3;
} srpacked;

extern svif sd_vif;

#endif
