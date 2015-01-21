#ifndef SD_MERGE_H_
#define SD_MERGE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sdmerge sdmerge;

struct sdmerge {
	uint32_t parent;
	sdindex index;
	sriter *merge;
	sriter i;
	uint32_t size_key;
	uint32_t size_stream;
	uint32_t size_page;
	uint64_t size_node;
	uint32_t processed;
	uint64_t offset;
	sr *r;
	sdbuild *build;
};

int sd_mergeinit(sdmerge*, sr*, uint32_t, sriter*,
                 sdbuild*, uint64_t,
                 uint32_t, uint32_t,
                 uint64_t, uint32_t, int, uint64_t);
int sd_mergefree(sdmerge*);
int sd_merge(sdmerge*);
int sd_mergecommit(sdmerge*, sdid*);

#endif
