#ifndef SD_IO_H_
#define SD_IO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdio sdio;

struct sdio {
	ssbuf    buf;
	int      direct;
	uint32_t size_page;
	uint32_t size_align;
};

static inline uint64_t
sd_iosize(sdio *s, ssfile *f) {
	return f->size + (ss_bufused(&s->buf) - s->size_align);
}

int sd_ioinit(sdio*);
int sd_iofree(sdio*, sr*);
int sd_ioprepare(sdio*, sr*, int, uint32_t, uint32_t);
int sd_ioreset(sdio*);
int sd_ioflush(sdio*, sr*, ssfile*);
int sd_iowrite(sdio*, sr*, ssfile*, char*, int);

#endif
