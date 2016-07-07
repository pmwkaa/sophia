#ifndef SD_DIRECTIO_H_
#define SD_DIRECTIO_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sddirectio sddirectio;

struct sddirectio {
	ssbuf    buf;
	uint32_t size_page;
	uint32_t size_align;
};

static inline uint64_t
sd_directio_size(sddirectio *s) {
	return ss_bufused(&s->buf) - s->size_align;
}

static inline int
sd_directio_inuse(sddirectio *s) {
	return s && s->size_page != 0;
}

int sd_directio_init(sddirectio*);
int sd_directio_free(sddirectio*, sr*);
int sd_directio_prepare(sddirectio*, sr*, uint32_t, uint32_t);
int sd_directio_reset(sddirectio*);
int sd_directio_write(sddirectio*, ssfile*, sr*, char*, int);
int sd_directio_flush(sddirectio*, ssfile*);

#endif
