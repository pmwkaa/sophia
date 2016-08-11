#ifndef SI_SCHEME_H_
#define SI_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sicompaction sicompaction;
typedef struct sischeme sischeme;

struct sicompaction {
	uint32_t branch_wm;
	uint32_t expire_period;
	uint64_t expire_period_us;
	uint32_t gc_period;
	uint64_t gc_period_us;
	uint32_t gc_wm;
};

struct sischeme {
	uint32_t      id;
	char         *name;
	char         *path;
	char         *path_backup;
	uint32_t      mmap;
	uint32_t      direct_io;
	uint32_t      direct_io_page_size;
	uint32_t      direct_io_buffer_size;
	sicompaction  compaction;
	uint32_t      sync;
	uint64_t      memory_limit;
	uint64_t      node_size;
	uint32_t      node_page_size;
	uint32_t      node_page_checksum;
	uint32_t      expire;
	uint32_t      compression;
	char         *compression_sz;
	ssfilterif   *compression_if;
	uint32_t      buf_gc_wm;
	sfupsert      upsert;
	sfscheme      scheme;
	srversion     version;
	srversion     version_storage;
};

void si_schemeinit(sischeme*);
void si_schemefree(sischeme*, sr*);
int  si_schemedeploy(sischeme*, sr*);
int  si_schemerecover(sischeme*, sr*);

#endif
