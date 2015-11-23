#ifndef SI_BRANCH_H_
#define SI_BRANCH_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sibranch sibranch;

struct sibranch {
	sdid id;
	sdindex index;
	ssblob copy;
	sibranch *link;
	sibranch *next;
};

static inline void
si_branchinit(sibranch *b, sr *r)
{
	memset(&b->id, 0, sizeof(b->id));
	sd_indexinit(&b->index);
	ss_blobinit(&b->copy, r->vfs);
	b->link = NULL;
	b->next = NULL;
}

static inline sibranch*
si_branchnew(sr *r)
{
	sibranch *b = (sibranch*)ss_malloc(r->a, sizeof(sibranch));
	if (ssunlikely(b == NULL)) {
		sr_oom_malfunction(r->e);
		return NULL;
	}
	si_branchinit(b, r);
	return b;
}

static inline void
si_branchset(sibranch *b, sdindex *i)
{
	b->id = i->h->id;
	b->index = *i;
}

static inline void
si_branchfree(sibranch *b, sr *r)
{
	sd_indexfree(&b->index, r);
	ss_blobfree(&b->copy);
	ss_free(r->a, b);
}

static inline int
si_branchis_root(sibranch *b) {
	return b->next == NULL;
}

static inline int
si_branchload(sibranch *b, sr *r, ssfile *file)
{
	sdindexheader *h = b->index.h;
	uint64_t offset = h->offset - h->total - sizeof(sdseal);
	uint64_t size   = h->total + sizeof(sdseal) + sizeof(sdindexheader) +
	                  h->size + h->extension;
	assert(b->copy.s == NULL);
	int rc;
	rc = ss_blobensure(&b->copy, size);
	if (ssunlikely(rc == -1))
		return sr_oom_malfunction(r->e);
	rc = ss_filepread(file, offset, b->copy.s, size);
	if (ssunlikely(rc == -1)) {
		sr_malfunction(r->e, "db file '%s' read error: %s",
		               ss_pathof(&file->path), strerror(errno));
		return -1;
	}
	ss_blobadvance(&b->copy, size);
	return 0;
}

#endif
