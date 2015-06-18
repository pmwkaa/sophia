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

struct sischeme {
	uint32_t    id;
	char       *name;
	char       *path;
	int         path_fail_on_exists;
	char       *path_backup;
	uint32_t    sync;
	uint64_t    node_size;
	uint32_t    node_page_size;
	uint32_t    node_page_checksum;
	uint32_t    compression;
	char       *compression_sz;
	ssfilterif *compression_if;
	uint32_t    compression_key;
	char       *fmt_sz;
	sf          fmt;
	sfstorage   fmt_storage;
	sfupdate    fmt_update;
	srscheme    scheme;
};

void si_schemeinit(sischeme*);
void si_schemefree(sischeme*, sr*);
int si_schemedeploy(sischeme*, sr*);
int si_schemerecover(sischeme*, sr*);

#endif
