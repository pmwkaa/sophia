
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
#include <libsv.h>
#include <libsd.h>
#include <libsi.h>

enum {
	SI_SCHEME_NONE,
	SI_SCHEME_NAME,
	SI_SCHEME_FORMAT,
	SI_SCHEME_FORMAT_STORAGE,
	SI_SCHEME_SCHEME,
	SI_SCHEME_NODE_SIZE,
	SI_SCHEME_NODE_PAGE_SIZE,
	SI_SCHEME_NODE_PAGE_CHECKSUM,
	SI_SCHEME_SYNC,
	SI_SCHEME_COMPRESSION,
	SI_SCHEME_COMPRESSION_KEY
};

void si_schemeinit(sischeme *s)
{
	memset(s, 0, sizeof(*s));
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
	if (s->compression_sz) {
		ss_free(r->a, s->compression_sz);
		s->compression_sz = NULL;
	}
	if (s->fmt_sz) {
		ss_free(r->a, s->fmt_sz);
		s->fmt_sz = NULL;
	}
	sr_schemefree(&s->scheme, r->a);
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
	rc = sd_schemeadd(&c, r, SI_SCHEME_NAME, SS_STRING, s->name,
	                  strlen(s->name) + 1);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sr_schemesave(&s->scheme, r->a, &buf);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_SCHEME, SS_STRING, buf.s,
	                  ss_bufused(&buf));
	if (ssunlikely(rc == -1))
		goto error;
	ss_buffree(&buf, r->a);
	uint32_t v = s->fmt;
	rc = sd_schemeadd(&c, r, SI_SCHEME_FORMAT, SS_U32, &v, sizeof(v));
	if (ssunlikely(rc == -1))
		goto error;
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
	rc = sd_schemeadd(&c, r, SI_SCHEME_COMPRESSION, SS_STRING,
	                  s->compression_if->name,
	                  strlen(s->compression_if->name) + 1);
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemeadd(&c, r, SI_SCHEME_COMPRESSION_KEY, SS_U32,
	                  &s->compression_key,
	                  sizeof(s->compression_key));
	if (ssunlikely(rc == -1))
		goto error;
	rc = sd_schemecommit(&c, r);
	if (ssunlikely(rc == -1))
		return -1;
	char path[PATH_MAX];
	snprintf(path, sizeof(path), "%s/scheme", s->path);
	rc = sd_schemewrite(&c, r, path, s->sync);
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
		case SI_SCHEME_FORMAT:
			s->fmt = sd_schemeu32(opt);
			char *name;
			if (s->fmt == SF_KV)
				name = "kv";
			else
			if (s->fmt == SF_DOCUMENT)
				name = "document";
			else
				goto error;
			ss_free(r->a, s->fmt_sz);
			s->fmt_sz = ss_strdup(r->a, name);
			if (ssunlikely(s->fmt_sz == NULL))
				goto error;
			break;
		case SI_SCHEME_FORMAT_STORAGE:
			s->fmt_storage = sd_schemeu32(opt);
			break;
		case SI_SCHEME_SCHEME: {
			sr_schemefree(&s->scheme, r->a);
			sr_schemeinit(&s->scheme);
			ssbuf buf;
			ss_bufinit(&buf);
			rc = sr_schemeload(&s->scheme, r->a, sd_schemesz(opt), opt->size);
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
		case SI_SCHEME_COMPRESSION_KEY:
			s->compression_key = sd_schemeu32(opt);
			break;
		case SI_SCHEME_COMPRESSION: {
			char *name = sd_schemesz(opt);
			ssfilterif *cif = NULL;
			if (strcmp(name, "none") == 0)
				cif = &ss_nonefilter;
			else
			if (strcmp(name, "lz4") == 0)
				cif = &ss_lz4filter;
			else
			if (strcmp(name, "zstd") == 0)
				cif = &ss_zstdfilter;
			else
				goto error;
			s->compression_if = cif;
			s->compression = s->compression_if != &ss_nonefilter;
			ss_free(r->a, s->compression_sz);
			s->compression_sz = ss_strdup(r->a, cif->name);
			if (ssunlikely(s->compression_sz == NULL))
				goto error;
			break;
		}
		}
		ss_iternext(sd_schemeiter, &i);
	}
	sd_schemefree(&c, r);
	return 0;
error:
	sd_schemefree(&c, r);
	return -1;
}
