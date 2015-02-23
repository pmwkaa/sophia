#ifndef SR_CRC_H_
#define SR_CRC_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef uint32_t (*srcrcf)(uint32_t, const void*, int);

srcrcf sr_crc32c_function(void);

#define sr_crcp(F, p, size, crc) \
	F(crc, p, size)

#define sr_crcs(F, p, size, crc) \
	F(crc, (char*)p + sizeof(uint32_t), size - sizeof(uint32_t))

#endif
