#ifndef SP_FILE_H_
#define SP_FILE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct spbatch spbatch;
typedef struct spfile spfile;

#define SPIOVMAX 1024

struct spbatch {
	struct iovec iov[SPIOVMAX];
	int iovc;
};

struct spfile {
	spa *a;
	int creat;
	uint64_t used;
	uint64_t size;
	uint64_t svp;
	char *file;
	int fd;
	char *map;
};

int sp_fileexists(char*);
int sp_filerm(char*);

static inline int
sp_epochrm(char *dir, uint32_t epoch, char *ext) {
	char path[1024];
	snprintf(path, sizeof(path), "%s/%"PRIu32".%s", dir, epoch, ext);
	return sp_filerm(path);
}

static inline void
sp_fileinit(spfile *f, spa *a) {
	memset(f, 0, sizeof(*f));
	f->a = a;
	f->fd = -1;
}

static inline void
sp_filesvp(spfile *f) {
	f->svp = f->used;
}

int sp_mapopen(spfile*, char*);
int sp_mapnew(spfile*, char*, uint64_t);
int sp_mapclose(spfile*);
int sp_mapcomplete(spfile*);
int sp_mapunmap(spfile*);
int sp_mapunlink(spfile*);
int sp_mapensure(spfile*, uint64_t, float);

static inline int
sp_mapepoch(spfile *f, char *dir, uint32_t epoch, char *ext) {
	char path[1024];
	snprintf(path, sizeof(path), "%s/%"PRIu32".%s", dir, epoch, ext);
	return sp_mapopen(f, path);
}

static inline int
sp_mapepochnew(spfile *f, uint64_t size,
               char *dir, uint32_t epoch, char *ext) {
	char path[1024];
	snprintf(path, sizeof(path), "%s/%"PRIu32".%s.incomplete", dir, epoch, ext);
	return sp_mapnew(f, path, size);
}

static inline void
sp_mapuse(spfile *f, size_t size) {
	f->used += size;
	assert(f->used <= f->size);
}

static inline void
sp_maprlb(spfile *f) {
	f->used = f->svp;
}

static inline int
sp_mapinbound(spfile *f, size_t off) {
	return off <= f->size;
}

static inline void
sp_batchinit(spbatch *b) {
	b->iovc = 0;
}

static inline int
sp_batchhas(spbatch *b) {
	return b->iovc > 0;
}

static inline int
sp_batchensure(spbatch *b, int count) {
	return (b->iovc + count) < SPIOVMAX;
}

static inline void
sp_batchadd(spbatch *b, void *ptr, size_t size) {
	assert(b->iovc < SPIOVMAX);
	b->iov[b->iovc].iov_base = ptr;
	b->iov[b->iovc].iov_len = size;
	b->iovc++;
}

int sp_lognew(spfile*, char*, uint32_t);
int sp_logcontinue(spfile*, char*, uint32_t);
int sp_logclose(spfile*);
int sp_logcomplete(spfile*);
int sp_logcompleteforce(spfile*);
int sp_logunlink(spfile*);
int sp_logwrite(spfile*, void*, size_t);
int sp_logput(spfile*, spbatch*);
int sp_logrlb(spfile*);
int sp_logeof(spfile*);

int sp_lockfile(spfile*, char*);
int sp_unlockfile(spfile*);

#endif
