
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>
#include <libsv.h>
#include <libsd.h>

typedef struct sdrecover sdrecover;

struct sdrecover {
	srfile *file;
	int corrupt;
	sdindexheader *actual;
	sdindexheader *v;
	srmap map;
} srpacked;

static int
sd_recovernext_of(sriter *i, sdindexheader *next)
{
	sdrecover *ri = (sdrecover*)i->priv;
	if (next == NULL)
		return 0;
	char *eof   = (char*)ri->map.p + ri->map.size;
	char *start = (char*)next;
	/* eof */
	if (srunlikely(start == eof)) {
		ri->v = NULL;
		return 0;
	}
	/* validate crc */
	uint32_t crc = sr_crcs(i->r->crc, next, sizeof(sdindexheader), 0);
	if (next->crc != crc) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index crc",
		               ri->file->file);
		ri->corrupt = 1;
		ri->v = NULL;
		return -1;
	}
	/* check version */
	if (! sr_versioncheck(&next->version))
		return sr_malfunction(i->r->e, "bad db file '%s' version",
		                      ri->file->file);
	char *end = start + sizeof(sdindexheader) + next->size +
	            next->total +
	            next->extension + sizeof(sdseal);
	if (srunlikely((start > eof || (end > eof)))) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad record size",
		               ri->file->file);
		ri->corrupt = 1;
		ri->v = NULL;
		return -1;
	}
	/* check seal */
	sdseal *s = (sdseal*)(end - sizeof(sdseal));
	int rc = sd_sealvalidate(s, i->r, next);
	if (srunlikely(rc == -1)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad seal",
		               ri->file->file);
		ri->corrupt = 1;
		ri->v = NULL;
		return -1;
	}
	ri->actual = next;
	ri->v = next;
	return 1;
}

int sd_recover_open(sriter *i, srfile *file)
{
	sdrecover *ri = (sdrecover*)i->priv;
	memset(ri, 0, sizeof(*ri));
	ri->file = file;
	if (srunlikely(ri->file->size < (sizeof(sdindexheader) + sizeof(sdseal)))) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad size",
		               ri->file->file);
		ri->corrupt = 1;
		return -1;
	}
	int rc = sr_map(&ri->map, ri->file->fd, ri->file->size, 1);
	if (srunlikely(rc == -1)) {
		sr_malfunction(i->r->e, "failed to mmap db file '%s': %s",
		               ri->file->file, strerror(errno));
		return -1;
	}
	sdindexheader *next = (sdindexheader*)((char*)ri->map.p);
	rc = sd_recovernext_of(i, next);
	if (srunlikely(rc == -1))
		sr_mapunmap(&ri->map);
	return rc;
}

static void
sd_recoverclose(sriter *i srunused)
{
	sdrecover *ri = (sdrecover*)i->priv;
	sr_mapunmap(&ri->map);
}

static int
sd_recoverhas(sriter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	return ri->v != NULL;
}

static void*
sd_recoverof(sriter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	return ri->v;
}

static void
sd_recovernext(sriter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	if (srunlikely(ri->v == NULL))
		return;
	sdindexheader *next =
		(sdindexheader*)((char*)ri->v +
		    (sizeof(sdindexheader) + ri->v->size) +
		     ri->v->total +
		     ri->v->extension + sizeof(sdseal));
	sd_recovernext_of(i, next);
}

sriterif sd_recover =
{
	.close   = sd_recoverclose,
	.has     = sd_recoverhas,
	.of      = sd_recoverof,
	.next    = sd_recovernext
};

int sd_recover_complete(sriter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	if (srunlikely(ri->actual == NULL))
		return -1;
	if (srlikely(ri->corrupt == 0))
		return  0;
	/* truncate file to the latest actual index */
	char *eof =
		(char*)ri->actual + sizeof(sdindexheader) +
		       ri->actual->size +
		       ri->actual->total +
		       ri->actual->extension + sizeof(sdseal);
	uint64_t file_size = eof - ri->map.p;
	int rc = sr_fileresize(ri->file, file_size);
	if (srunlikely(rc == -1))
		return -1;
	sr_errorreset(i->r->e);
	return 0;
}
