
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libso.h>
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

enum {
	SI_SCHEME_NONE,
	SI_SCHEME_VERSION,
	SI_SCHEME_VERSION_STORAGE,
	SI_SCHEME_NAME,
	SI_SCHEME_FORMAT_STORAGE,
	SI_SCHEME_SCHEME,
	SI_SCHEME_NODE_SIZE,
	SI_SCHEME_NODE_PAGE_SIZE,
	SI_SCHEME_NODE_PAGE_CHECKSUM,
	SI_SCHEME_SYNC,
	SI_SCHEME_COMPRESSION_COLD,
	SI_SCHEME_COMPRESSION_COPY,
	SI_SCHEME_COMPRESSION_HOT,
	SI_SCHEME_COMPRESSION_RESERVED0,
	SI_SCHEME_COMPRESSION_RESERVED1,
	SI_SCHEME_AMQF,
	SI_SCHEME_CACHE_MODE,
	SI_SCHEME_EXPIRE
};

static inline void
si_schemecompaction_init(sicompaction *c)
{
	c->mode              = 0; /* 0: create branches, 1: compact from memory */
	c->checkpoint_wm     = 90;
	c->compact_wm        = 2;
	c->compact_mode      = 0; /* 0: temperature, 1: branch */
	c->branch_wm         = 10 * 1024 * 1024;
	c->branch_age        = 0;
	c->branch_age_period = 0;
	c->branch_age_wm     = 1 * 1024 * 1024;
	c->anticache_period  = 0;
	c->snapshot_period   = 0;
	c->expire_period     = 0;
	c->gc_period         = 60;
	c->gc_wm             = 30;
	c->lru_period        = 0;
}

void si_schemeinit(sischeme *s)
{
	memset(s, 0, sizeof(*s));
	sr_version(&s->version);
	sr_version_storage(&s->version_storage);
	si_schemecompaction_init(&s->compaction);
}

void si_schemefree(sischeme *s, sr *r)
{
	if (s->name) {
		ss_free(r->a, s->name);
		s->name = NULL;
	}
	if (s->path) {
		ss_free(r->a, s->path);
		s->path = NULL;
	}
	if (s->path_backup) {
		ss_free(r->a, s->path_backup);
		s->path_backup = NULL;
	}
	if (s->storage_sz) {
		ss_free(r->a, s->storage_sz);
		s->storage_sz = NULL;
	}
	if (s->compression_cold_sz) {
		ss_free(r->a, s->compression_cold_sz);
		s->compression_cold_sz = NULL;
	}
	if (s->compression_hot_sz) {
		ss_free(r->a, s->compression_hot_sz);
		s->compression_hot_sz = NULL;
	}
	sf_schemefree(&s->scheme, r->a);
}

int si_schemedeploy(sischeme *s, sr *r)
{
	sdscheme c;
	sd_schemeinit(&c);
	int rc;
	rc = sd_schemebegin(&c, r);
	if (ssunlikely(rc == -1))
		return -1;
	ssbuf buf;
	ss_bufinit(&buf);
	rc = sd_schemeadd(&c, r, SI_SCHEME_VERSION, SS_STRING, &s->version,
	                  sizeof(s->version));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_VERSION_STORAGE, SS_STRING,
	                  &s->version_storage, sizeof(s->version_storage));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_NAME, SS_STRING, s->name,
	                  strlen(s->name) + 1);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sf_schemesave(&s->scheme, r->a, &buf);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_SCHEME, SS_STRING, buf.s,
	                  ss_bufused(&buf));
	if (ssunlikely(rc == -1))
		goto error;
	ss_buffree(&buf, r->a);
	uint32_t v;
	v = s->fmt_storage;
	rc = sd_schemeadd(&c, r, SI_SCHEME_FORMAT_STORAGE, SS_U32, &v, sizeof(v));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_NODE_SIZE, SS_U64,
	                  &s->node_size,
	                  sizeof(s->node_size));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_NODE_PAGE_SIZE, SS_U32,
	                  &s->node_page_size,
	                  sizeof(s->node_page_size));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_NODE_PAGE_CHECKSUM, SS_U32,
	                  &s->node_page_checksum,
	                  sizeof(s->node_page_checksum));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_SYNC, SS_U32,
	                  &s->sync,
	                  sizeof(s->sync));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_COMPRESSION_COLD, SS_STRING,
	                  s->compression_cold_if->name,
	                  strlen(s->compression_cold_if->name) + 1);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_COMPRESSION_HOT, SS_STRING,
	                  s->compression_hot_if->name,
	                  strlen(s->compression_hot_if->name) + 1);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_COMPRESSION_COPY, SS_U32,
	                  &s->compression_copy,
	                  sizeof(s->compression_copy));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_AMQF, SS_U32,
	                  &s->amqf, sizeof(s->amqf));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_EXPIRE, SS_U32,
	                  &s->expire, sizeof(s->expire));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemecommit(&c, r);
	if (ssunlikely(rc == -1))
		return -1;
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/scheme", s->path);
	rc = sd_schemewrite(&c, r, path, 0);
	sd_schemefree(&c, r);
	return rc;
error:
	ss_buffree(&buf, r->a);
	sd_schemefree(&c, r);
	return -1;
}

int si_schemerecover(sischeme *s, sr *r)
{
	sdscheme c;
	sd_schemeinit(&c);
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/scheme", s->path);
	int version_storage_set = 0;
	int rc;
	rc = sd_schemerecover(&c, r, path);
	if (ssunlikely(rc == -1))
		goto error;
	ssiter i;
	ss_iterinit(sd_schemeiter, &i);
	rc = ss_iteropen(sd_schemeiter, &i, r, &c, 1);
	if (ssunlikely(rc == -1))
		goto error;
	while (ss_iterhas(sd_schemeiter, &i))
	{
		sdschemeopt *opt = ss_iterof(sd_schemeiter, &i);
		switch (opt->id) {
		case SI_SCHEME_VERSION:
			break;
		case SI_SCHEME_VERSION_STORAGE: {
			if (opt->size != sizeof(srversion))
				goto error;
			srversion *version = (srversion*)sd_schemesz(opt);
			if (! sr_versionstorage_check(version))
				goto error_format;
			version_storage_set = 1;
			break;
		}
		case SI_SCHEME_FORMAT_STORAGE:
			s->fmt_storage = sd_schemeu32(opt);
			break;
		case SI_SCHEME_SCHEME: {
			sf_schemefree(&s->scheme, r->a);
			sf_schemeinit(&s->scheme);
			ssbuf buf;
			ss_bufinit(&buf);
			rc = sf_schemeload(&s->scheme, r->a, sd_schemesz(opt), opt->size);
			if (ssunlikely(rc == -1))
				goto error;
			rc = sf_schemevalidate(&s->scheme, r->a);
			if (ssunlikely(rc == -1))
				goto error;
			ss_buffree(&buf, r->a);
			break;
		}
		case SI_SCHEME_NODE_SIZE:
			s->node_size = sd_schemeu64(opt);
			break;
		case SI_SCHEME_NODE_PAGE_SIZE:
			s->node_page_size = sd_schemeu32(opt);
			break;
		case SI_SCHEME_COMPRESSION_COPY:
			s->compression_copy = sd_schemeu32(opt);
			break;
		case SI_SCHEME_COMPRESSION_COLD: {
			char *name = sd_schemesz(opt);
			ssfilterif *cif = ss_filterof(name);
			if (ssunlikely(cif == NULL))
				goto error;
			s->compression_cold_if = cif;
			s->compression_cold = s->compression_cold_if != &ss_nonefilter;
			ss_free(r->a, s->compression_cold_sz);
			s->compression_cold_sz = ss_strdup(r->a, cif->name);
			if (ssunlikely(s->compression_cold_sz == NULL))
				goto error;
			break;
		}
		case SI_SCHEME_COMPRESSION_HOT: {
			char *name = sd_schemesz(opt);
			ssfilterif *cif = ss_filterof(name);
			if (ssunlikely(cif == NULL))
				goto error;
			s->compression_hot_if = cif;
			s->compression_hot = s->compression_hot_if != &ss_nonefilter;
			ss_free(r->a, s->compression_hot_sz);
			s->compression_hot_sz = ss_strdup(r->a, cif->name);
			if (ssunlikely(s->compression_hot_sz == NULL))
				goto error;
			break;
		}
		case SI_SCHEME_AMQF:
			s->amqf = sd_schemeu32(opt);
			break;
		case SI_SCHEME_EXPIRE:
			s->expire = sd_schemeu32(opt);
			break;
		default: /* skip unknown */
			break;
		}
		ss_iternext(sd_schemeiter, &i);
	}
	if (ssunlikely(! version_storage_set))
		goto error_format;
	sd_schemefree(&c, r);
	return 0;
error_format:
	sr_error(r->e, "%s", "incompatible storage format version");
error:
	sd_schemefree(&c, r);
	return -1;
}
