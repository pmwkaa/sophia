#ifndef SS_CRC_H_
#define SS_CRC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef uint32_t (*sscrcf)(uint32_t, const void*, int);

sscrcf ss_crc32c_function(void);

#define ss_crcp(F, p, size, crc) \
	F(crc, p, size)

#define ss_crcs(F, p, size, crc) \
	F(crc, (char*)p + sizeof(uint32_t), size - sizeof(uint32_t))

#endif
