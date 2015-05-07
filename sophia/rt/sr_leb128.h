#ifndef SR_LEB128_H_
#define SR_LEB128_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline int
sr_leb128write(unsigned char *dest, uint64_t value)
{
	int size = 0;
	do {
		uint8_t byte = value & 0x7F;
		value >>= 7;
		if (value != 0)
			byte |= 0x80;
		dest[size++] = byte;
	} while (value != 0);
	return size;
}

static inline int
sr_leb128read(unsigned char *src, uint64_t *value)
{
	unsigned char *start = src;
	int lsh = 0;
	*value = 0;
	do {
		*value |= ((uint64_t)(*src & 0x7F)) << lsh;
		lsh += 7;
	} while (*(src++) >= 128);
	return src - start;
}

#endif
