#ifndef SR_CRC_H_
#define SR_CRC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

uint32_t sr_crc32c(uint32_t, const void*, int);

static inline uint32_t
sr_crcp(const void *p, int size, uint32_t crc)
{
	return sr_crc32c(crc, p, size);
}

static inline uint32_t
sr_crcs(const void *s, int size, uint32_t crc)
{
	return sr_crc32c(crc, (char*)s + sizeof(uint32_t),
	                 size - sizeof(uint32_t));
}

#endif
