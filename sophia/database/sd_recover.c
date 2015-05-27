
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

typedef struct sdrecover sdrecover;

struct sdrecover {
	ssfile *file;
	int corrupt;
	sdindexheader *actual;
	sdindexheader *v;
	ssmap map;
	sr *r;
} sspacked;

static int
sd_recovernext_of(sdrecover *i, sdindexheader *next)
{
	if (next == NULL)
		return 0;
	char *eof   = (char*)i->map.p + i->map.size;
	char *start = (char*)next;
	/* eof */
	if (ssunlikely(start == eof)) {
		i->v = NULL;
		return 0;
	}
	/* validate crc */
	uint32_t crc = ss_crcs(i->r->crc, next, sizeof(sdindexheader), 0);
	if (next->crc != crc) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index crc",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	/* check version */
	if (! sr_versioncheck(&next->version))
		return sr_malfunction(i->r->e, "bad db file '%s' version",
		                      i->file->file);
	char *end = start + sizeof(sdindexheader) + next->size +
	            next->total +
	            next->extension + sizeof(sdseal);
	if (ssunlikely((start > eof || (end > eof)))) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad record size",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	/* check seal */
	sdseal *s = (sdseal*)(end - sizeof(sdseal));
	int rc = sd_sealvalidate(s, i->r, next);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad seal",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	i->actual = next;
	i->v = next;
	return 1;
}

int sd_recover_open(ssiter *i, sr *r, ssfile *file)
{
	sdrecover *ri = (sdrecover*)i->priv;
	memset(ri, 0, sizeof(*ri));
	ri->r = r;
	ri->file = file;
	if (ssunlikely(ri->file->size < (sizeof(sdindexheader) + sizeof(sdseal)))) {
		sr_malfunction(ri->r->e, "corrupted db file '%s': bad size",
		               ri->file->file);
		ri->corrupt = 1;
		return -1;
	}
	int rc = ss_map(&ri->map, ri->file->fd, ri->file->size, 1);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(ri->r->e, "failed to mmap db file '%s': %s",
		               ri->file->file, strerror(errno));
		return -1;
	}
	sdindexheader *next = (sdindexheader*)((char*)ri->map.p);
	rc = sd_recovernext_of(ri, next);
	if (ssunlikely(rc == -1))
		ss_mapunmap(&ri->map);
	return rc;
}

static void
sd_recoverclose(ssiter *i ssunused)
{
	sdrecover *ri = (sdrecover*)i->priv;
	ss_mapunmap(&ri->map);
}

static int
sd_recoverhas(ssiter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	return ri->v != NULL;
}

static void*
sd_recoverof(ssiter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	return ri->v;
}

static void
sd_recovernext(ssiter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	if (ssunlikely(ri->v == NULL))
		return;
	sdindexheader *next =
		(sdindexheader*)((char*)ri->v +
		    (sizeof(sdindexheader) + ri->v->size) +
		     ri->v->total +
		     ri->v->extension + sizeof(sdseal));
	sd_recovernext_of(ri, next);
}

ssiterif sd_recover =
{
	.close   = sd_recoverclose,
	.has     = sd_recoverhas,
	.of      = sd_recoverof,
	.next    = sd_recovernext
};

int sd_recover_complete(ssiter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	if (ssunlikely(ri->actual == NULL))
		return -1;
	if (sslikely(ri->corrupt == 0))
		return  0;
	/* truncate file to the latest actual index */
	char *eof =
		(char*)ri->actual + sizeof(sdindexheader) +
		       ri->actual->size +
		       ri->actual->total +
		       ri->actual->extension + sizeof(sdseal);
	uint64_t file_size = eof - ri->map.p;
	int rc = ss_fileresize(ri->file, file_size);
	if (ssunlikely(rc == -1))
		return -1;
	sr_errorreset(ri->r->e);
	return 0;
}
