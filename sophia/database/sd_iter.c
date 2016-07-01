
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

typedef struct sditer sditer;

struct sditer {
	ssfile        *file;
	int            corrupt;
	sdindexheader *v;
	sdindexheader *actual;
	sdseal        *seal;
	ssmmap         map;
	sr            *r;
} sspacked;

static int
sd_iternext_of(sditer *i, sdseal *seal)
{
	if (seal == NULL)
		return 0;

	char *eof = i->map.p + i->map.size;
	char *pointer = i->map.p + seal->index_offset;

	int sanity_check = 0;
	sanity_check += (pointer >= (char*)seal);
	sanity_check += (pointer + sizeof(sdindexheader)) > eof;

	/* validate index pointer */
	if (ssunlikely(sanity_check > 0)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad seal",
		               ss_pathof(&i->file->path));
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	sdindexheader *index = (sdindexheader*)(pointer);

	/* validate index crc */
	uint32_t crc = ss_crcs(i->r->crc, index, sizeof(sdindexheader), 0);
	if (index->crc != crc) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index crc",
		               ss_pathof(&i->file->path));
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}

	/* validate index size */
	char *end = pointer + sizeof(sdindexheader) + index->size +
	            index->extension;
	if (ssunlikely(end > eof)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad index size",
		               ss_pathof(&i->file->path));
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}

	/* validate seal */
	int rc = sd_sealvalidate(seal, i->r, index);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(i->r->e, "corrupted db file '%s': bad seal",
		               ss_pathof(&i->file->path));
		i->corrupt = 1;
		i->v = NULL;
		return -1;
	}
	i->seal   = seal;
	i->actual = index;
	i->v      = index;
	return 1;
}

int sd_iter_open(ssiter *i, sr *r, ssfile *file)
{
	sditer *ri = (sditer*)i->priv;
	memset(ri, 0, sizeof(*ri));
	ri->r = r;
	ri->file = file;
	if (ssunlikely(ri->file->size < (sizeof(sdseal) + sizeof(sdindexheader)))) {
		sr_malfunction(ri->r->e, "corrupted db file '%s': bad size",
		               ss_pathof(&ri->file->path));
		ri->corrupt = 1;
		return -1;
	}
	int rc = ss_vfsmmap(r->vfs, &ri->map, ri->file->fd, ri->file->size, 1);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(ri->r->e, "failed to mmap db file '%s': %s",
		               ss_pathof(&ri->file->path),
		               strerror(errno));
		return -1;
	}
	sdseal *seal =
		(sdseal*)((char*)ri->map.p +
		          (ri->file->size - sizeof(sdseal)));
	rc = sd_iternext_of(ri, seal);
	if (ssunlikely(rc == -1))
		ss_vfsmunmap(r->vfs, &ri->map);
	return rc;
}

static void
sd_iterclose(ssiter *i)
{
	sditer *ri = (sditer*)i->priv;
	ss_vfsmunmap(ri->r->vfs, &ri->map);
}

static int
sd_iterhas(ssiter *i)
{
	sditer *ri = (sditer*)i->priv;
	return ri->v != NULL;
}

static void*
sd_iterof(ssiter *i)
{
	sditer *ri = (sditer*)i->priv;
	return ri->v;
}

static void
sd_iternext(ssiter *i)
{
	sditer *ri = (sditer*)i->priv;
	if (ssunlikely(ri->v == NULL))
		return;
	char *next = (char*)ri->v - ri->v->total;
	if (next == ri->map.p) {
		ri->v = NULL;
		return;
	}
	next = next - sizeof(sdseal);
	sd_iternext_of(ri, (sdseal*)next);
}

ssiterif sd_iter =
{
	.close = sd_iterclose,
	.has   = sd_iterhas,
	.of    = sd_iterof,
	.next  = sd_iternext
};

#if 0
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
#endif

int sd_iter_iserror(ssiter *i)
{
	sditer *ri = (sditer*)i->priv;
	return ri->corrupt;
}

int sd_iter_isroot(ssiter *i)
{
	sditer *ri = (sditer*)i->priv;
	assert(ri->v != NULL);
	char *next = (char*)ri->v - ri->v->total;
	return next == ri->map.p;
}
