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
	uint32_t offset:30;
	uint8_t  flags:2;
} srpacked;

extern svif sd_vif;
extern svif sd_vrawif;

#endif
