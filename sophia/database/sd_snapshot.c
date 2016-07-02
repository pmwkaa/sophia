
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

int sd_snapshot_begin(sdsnapshot *s, sr *r)
{
	int rc = ss_bufensure(&s->buf, r->a, sizeof(sdsnapshotheader));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	sdsnapshotheader *h = sd_snapshot_header(s);
	memset(h, 0, sizeof(*h));
	ss_bufadvance(&s->buf, sizeof(*h));
	return 0;
}

int sd_snapshot_add(sdsnapshot *s, sr *r, uint64_t id,
                    uint64_t file_size,
                    uint32_t branch_count, uint64_t tr)
{
	int rc = ss_bufensure(&s->buf, r->a, sizeof(sdsnapshotnode));
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	s->current = (uint32_t)(s->buf.p - s->buf.s);
	sdsnapshotnode *n = (sdsnapshotnode*)s->buf.p;
	n->crc               = 0;
	n->id                = id;
	n->size_file         = file_size;
	n->size              = 0;
	n->branch_count      = branch_count;
	n->temperature_reads = tr;
	n->reserve[0]        = 0;
	n->reserve[1]        = 0;
	n->reserve[2]        = 0;
	n->reserve[3]        = 0;
	n->crc = ss_crcs(r->crc, (char*)n, sizeof(*n), 0);
	ss_bufadvance(&s->buf, sizeof(*n));
	sdsnapshotheader *h = sd_snapshot_header(s);
	h->nodes++;
	return 0;
}

int sd_snapshot_addbranch(sdsnapshot *s, sr *r, sdindexheader *h)
{
	int size = sd_indexsize_ext(h);
	int rc = ss_bufensure(&s->buf, r->a, size);
	if (ssunlikely(rc == -1))
		return sr_oom(r->e);
	char *start = (char*)h - (h->size + h->extension);
	memcpy(s->buf.p, start, size);
	ss_bufadvance(&s->buf, size);
	sdsnapshotnode *n = (sdsnapshotnode*)(s->buf.s + s->current);
	n->size += size;
	return 0;
}

int sd_snapshot_commit(sdsnapshot *s, sr *r,
                       uint64_t read_disk,
                       uint64_t read_cache)
{
	sdsnapshotheader *h = sd_snapshot_header(s);
	h->read_disk    = read_disk;
	h->read_cache   = read_cache;
	h->size         = ss_bufused(&s->buf);
	h->crc = ss_crcs(r->crc, (char*)h, sizeof(*h), 0);
	return 0;
}
