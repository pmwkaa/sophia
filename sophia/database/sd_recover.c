
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
	sdindexheader *v;
	sdindexheader *actual;
	sdseal *seal;
	ssmmap map;
	sr *r;
} sspacked;

static int
sd_recovernext_of(sdrecover *i, sdseal *next)
{
	if (next == NULL)
		return 0;

	char *eof = (char*)i->map.p + i->map.size;
	char *pointer = (char*)next;

	/* eof */
	if (ssunlikely(pointer == eof)) {
		i->v = NULL;
		return 0;
	}

	/* validate seal pointer */
	if (ssunlikely(((pointer + sizeof(sdseal)) > eof))) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad seal size",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	pointer = i->map.p + next->index_offset;

	/* validate index pointer */
	if (ssunlikely(((pointer + sizeof(sdindexheader)) > eof))) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index size",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	sdindexheader *index = (sdindexheader*)(pointer);

	/* validate index crc */
	uint32_t crc = ss_crcs(i->r->crc, index, sizeof(sdindexheader), 0);
	if (index->crc != crc) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index crc",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}

	/* validate index size */
	char *end = pointer + sizeof(sdindexheader) + index->size +
	            index->extension;
	if (ssunlikely(end > eof)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index size",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}

	/* validate seal */
	int rc = sd_sealvalidate(next, i->r, index);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad seal",
		               i->file->file);
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	i->seal = next;
	i->actual = index;
	i->v = index;
	return 1;
}

int sd_recover_open(ssiter *i, sr *r, ssfile *file)
{
	sdrecover *ri = (sdrecover*)i->priv;
	memset(ri, 0, sizeof(*ri));
	ri->r = r;
	ri->file = file;
	if (ssunlikely(ri->file->size < (sizeof(sdseal) + sizeof(sdindexheader)))) {
		sr_malfunction(ri->r->e, "corrupted db file '%s': bad size",
		               ri->file->file);
		ri->corrupt = 1;
		return -1;
	}
	int rc = ss_mmap(&ri->map, ri->file->fd, ri->file->size, 1);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(ri->r->e, "failed to mmap db file '%s': %s",
		               ri->file->file, strerror(errno));
		return -1;
	}
	sdseal *seal = (sdseal*)((char*)ri->map.p);
	rc = sd_recovernext_of(ri, seal);
	if (ssunlikely(rc == -1))
		ss_munmap(&ri->map);
	return rc;
}

static void
sd_recoverclose(ssiter *i ssunused)
{
	sdrecover *ri = (sdrecover*)i->priv;
	ss_munmap(&ri->map);
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
	sdseal *next =
		(sdseal*)((char*)ri->v +
		    (sizeof(sdindexheader) + ri->v->size) +
		     ri->v->extension);
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
	if (ssunlikely(ri->seal == NULL))
		return -1;
	if (sslikely(ri->corrupt == 0))
		return  0;
	/* truncate file to the end of a latest actual
	 * index */
	char *eof =
		(char*)ri->map.p +
		       ri->actual->offset + sizeof(sdindexheader) +
		       ri->actual->size +
		       ri->actual->extension;
	uint64_t file_size = eof - ri->map.p;
	int rc = ss_fileresize(ri->file, file_size);
	if (ssunlikely(rc == -1))
		return -1;
	sr_errorreset(ri->r->e);
	return 0;
}
