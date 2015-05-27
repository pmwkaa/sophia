#ifndef SS_LEB128_H_
#define SS_LEB128_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

static inline int
ss_leb128size(uint64_t value)
{
	int size = 0;
	do {
		value >>= 7;
		size++;
	} while (value != 0);
	return size;
}

static inline int
ss_leb128write(char *dest, uint64_t value)
{
	int size = 0;
	do {
		uint8_t byte = value & 0x7F;
		value >>= 7;
		if (value != 0)
			byte |= 0x80;
		((unsigned char*)dest)[size++] = byte;
	} while (value != 0);
	return size;
}

static inline int
ss_leb128read(char *ssc, uint64_t *value)
{
	unsigned char *start = (unsigned char*)ssc;
	int lsh = 0;
	*value = 0;
	do {
		*value |= ((uint64_t)(*(unsigned char*)ssc & 0x7F)) << lsh;
		lsh += 7;
	} while (*((unsigned char*)ssc++) >= 128);

	return (unsigned char*)ssc - start;
}

static inline int
ss_leb128skip(char *ssc)
{
	unsigned char *start = (unsigned char*)ssc;
	while (*((unsigned char*)ssc++) >= 128);
	return (unsigned char*)ssc - start;
}

#endif
