#ifndef SI_SCHEME_H_
#define SI_SCHEME_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sischeme sischeme;

typedef enum {
	SI_SCACHE,
	SI_SANTI_CACHE,
	SI_SIN_MEMORY
} sistorage;

struct sischeme {
	uint32_t    id;
	char       *name;
	char       *path;
	uint32_t    path_fail_on_exists;
	char       *path_backup;
	uint32_t    mmap;
	sistorage   storage;
	char       *storage_sz;
	uint32_t    sync;
	uint64_t    node_size;
	uint32_t    node_page_size;
	uint32_t    node_page_checksum;
	uint32_t    node_compact_load;
	uint32_t    expire;
	uint32_t    compression_cold;
	char       *compression_cold_sz;
	ssfilterif *compression_cold_if;
	uint32_t    compression_hot;
	char       *compression_hot_sz;
	ssfilterif *compression_hot_if;
	uint32_t    compression_copy;
	uint32_t    temperature;
	uint32_t    amqf;
	uint64_t    lru;
	uint32_t    lru_step;
	uint32_t    buf_gc_wm;
	sfstorage   fmt_storage;
	sfupsert    fmt_upsert;
	sfscheme    scheme;
	srversion   version;
	srversion   version_storage;
};

void si_schemeinit(sischeme*);
void si_schemefree(sischeme*, sr*);
int  si_schemedeploy(sischeme*, sr*);
int  si_schemerecover(sischeme*, sr*);

#endif
