#ifndef SD_MERGE_H_
#define SD_MERGE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdmergeconf sdmergeconf;
typedef struct sdmerge sdmerge;

struct sdmergeconf {
	uint32_t    write;
	uint32_t    stream;
	uint64_t    size_stream;
	uint64_t    size_node;
	uint32_t    size_page;
	uint32_t    checksum;
	uint32_t    expire;
	uint32_t    timestamp;
	uint32_t    compression;
	ssfilterif *compression_if;
	uint32_t    amqf;
	uint32_t    direct_io;
	uint32_t    direct_io_page_size;
	uint64_t    vlsn;
	uint32_t    save_delete;
	uint32_t    save_upsert;
};

struct sdmerge {
	sdindex     index;
	ssiter      *merge;
	ssiter      i;
	sdmergeconf *conf;
	sr          *r;
	sdbuild     *build;
	ssqf        *qf;
	uint64_t    processed;
	uint64_t    current;
	uint64_t    limit;
	int         resume;
};

int sd_mergeinit(sdmerge*, sr*, ssiter*, sdbuild*, ssqf*, svupsert*,
                 sdmergeconf*);
int sd_mergefree(sdmerge*);
int sd_merge(sdmerge*);
int sd_mergepage(sdmerge*, uint64_t);
int sd_mergecommit(sdmerge*, sdid*, uint64_t);

#endif
