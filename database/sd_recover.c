
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

static void
sd_recoverinit(sriter *i)
{
	assert(sizeof(sdrecover) <= sizeof(i->priv));
	sdrecover *ri = (sdrecover*)i->priv;
	memset(ri, 0, sizeof(*ri));
}

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
	uint32_t crc = sr_crcs(next, sizeof(sdindexheader), 0);
	if (next->crc != crc) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index crc",
		               ri->file->file);
		ri->corrupt = 1;
		ri->v = NULL;
		return -1;
	}
	char *end = start + sizeof(sdindexheader) +
	            next->count * next->block + next->total +
	            sizeof(sdseal);
	if (srunlikely((start > eof || (end > eof)))) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad record size",
		               ri->file->file);
		ri->corrupt = 1;
		ri->v = NULL;
		return -1;
	}
	/* check seal */
	sdseal *s = (sdseal*)(end - sizeof(sdseal));
	int rc = sd_sealvalidate(s, next);
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

static inline int
sd_recovervalidate(sriter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	sdindexheader *next = (sdindexheader*)((char*)ri->map.p);
	int rc = sd_recovernext_of(i, next);
	if (srunlikely(rc == -1))
		return -1;
	return 0;
}

static int
sd_recoveropen(sriter *i, va_list args)
{
	sdrecover *ri = (sdrecover*)i->priv;
	ri->file = va_arg(args, srfile*);
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
	ri->v = NULL;
	rc = sd_recovervalidate(i);
	if (srunlikely(rc == -1))
		sr_mapunmap(&ri->map);
	return 0;
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
		    (sizeof(sdindexheader) + ri->v->count * ri->v->block) +
		     ri->v->total + sizeof(sdseal));
	sd_recovernext_of(i, next);
}

sriterif sd_recover =
{
	.init    = sd_recoverinit,
	.open    = sd_recoveropen,
	.close   = sd_recoverclose,
	.has     = sd_recoverhas,
	.of      = sd_recoverof,
	.next    = sd_recovernext
};

int sd_recovercomplete(sriter *i)
{
	sdrecover *ri = (sdrecover*)i->priv;
	if (srunlikely(ri->actual == NULL))
		return -1;
	if (srlikely(ri->corrupt == 0))
		return  0;
	/* truncate file to the latest actual index */
	char *eof =
		(char*)ri->actual + sizeof(sdindexheader) +
		       ri->actual->count * ri->actual->block +
		       ri->actual->total + sizeof(sdseal);
	uint64_t file_size = eof - ri->map.p;
	int rc = sr_fileresize(ri->file, file_size);
	if (srunlikely(rc == -1))
		return -1;
	sr_errorreset(i->r->e);
	return 0;
}
