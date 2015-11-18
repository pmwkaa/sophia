#ifndef SD_SNAPSHOT_H_
#define SD_SNAPSHOT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdsnapshotheader sdsnapshotheader;
typedef struct sdsnapshotnode sdsnapshotnode;
typedef struct sdsnapshot sdsnapshot;

struct sdsnapshotheader {
	uint32_t crc;
	uint32_t size;
	uint32_t nodes;
	uint64_t read_disk;
	uint64_t read_cache;
	uint64_t reserve[4];
} sspacked;

struct sdsnapshotnode {
	uint32_t crc;
	uint64_t id;
	uint64_t size_file;
	uint32_t size;
	uint32_t branch_count;
	uint64_t temperature_reads;
	uint64_t reserve[4];
	/* sdindexheader[] */
} sspacked;

struct sdsnapshot {
	uint32_t current;
	ssbuf buf;
};

static inline void
sd_snapshot_init(sdsnapshot *s)
{
	s->current = 0;
	ss_bufinit(&s->buf);
}

static inline void
sd_snapshot_free(sdsnapshot *s, sr *r)
{
	ss_buffree(&s->buf, r->a);
}

static inline sdsnapshotheader*
sd_snapshot_header(sdsnapshot *s) {
	return (sdsnapshotheader*)s->buf.s;
}

static inline int
sd_snapshot_is(sdsnapshot *s) {
	return s->buf.s != NULL;
}

int sd_snapshot_begin(sdsnapshot*, sr*);
int sd_snapshot_add(sdsnapshot*, sr*, uint64_t, uint64_t, uint32_t, uint64_t);
int sd_snapshot_addbranch(sdsnapshot*, sr*, sdindexheader*);
int sd_snapshot_commit(sdsnapshot*, sr*, uint64_t, uint64_t);

#endif
