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
	uint32_t crc;
	uint64_t lsn;
	uint8_t  flags;
	uint32_t valuesize;
	uint32_t valueoffset;
	uint16_t keysize;
	char     key[];
} srpacked;

extern svif sd_vif;

static inline int
sd_isdb(sv *v) {
	return v->i == &sd_vif;
}

static inline char*
sd_vkey(sdv *v) {
	return (char*)(v) + sizeof(sdv);
}

static inline void*
sd_vvalue(sdv *v) {
	return (char*)(v) + sizeof(sdv) + v->keysize;
}

#endif
