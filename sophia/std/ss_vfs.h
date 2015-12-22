#ifndef SS_VFS_H_
#define SS_VFS_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssvfsif ssvfsif;
typedef struct ssvfs ssvfs;

struct ssvfsif {
	int     (*init)(ssvfs*, va_list);
	void    (*free)(ssvfs*);
	int64_t (*size)(ssvfs*, char*);
	int     (*exists)(ssvfs*, char*);
	int     (*unlink)(ssvfs*, char*);
	int     (*rename)(ssvfs*, char*, char*);
	int     (*mkdir)(ssvfs*, char*, int);
	int     (*rmdir)(ssvfs*, char*);
	int     (*open)(ssvfs*, char*, int, int);
	int     (*close)(ssvfs*, int);
	int     (*sync)(ssvfs*, int);
	int     (*advise)(ssvfs*, int, int, uint64_t, uint64_t);
	int     (*truncate)(ssvfs*, int, uint64_t);
	int64_t (*pread)(ssvfs*, int, uint64_t, void*, int);
	int64_t (*pwrite)(ssvfs*, int, uint64_t, void*, int);
	int64_t (*write)(ssvfs*, int, void*, int);
	int64_t (*writev)(ssvfs*, int, ssiov*);
	int64_t (*seek)(ssvfs*, int, uint64_t);
	int     (*mmap)(ssvfs*, ssmmap*, int, uint64_t, int);
	int     (*mmap_allocate)(ssvfs*, ssmmap*, uint64_t);
	int     (*mremap)(ssvfs*, ssmmap*, uint64_t);
	int     (*munmap)(ssvfs*, ssmmap*);
};

struct ssvfs {
	ssvfsif *i;
	char priv[48];
};

static inline int
ss_vfsinit(ssvfs *f, ssvfsif *i, ...)
{
	f->i = i;
	va_list args;
	va_start(args, i);
	int rc = i->init(f, args);
	va_end(args);
	return rc;
}

static inline void
ss_vfsfree(ssvfs *f)
{
	f->i->free(f);
}

#define ss_vfssize(fs, path)                 (fs)->i->size(fs, path)
#define ss_vfsexists(fs, path)               (fs)->i->exists(fs, path)
#define ss_vfsunlink(fs, path)               (fs)->i->unlink(fs, path)
#define ss_vfsrename(fs, src, dest)          (fs)->i->rename(fs, src, dest)
#define ss_vfsmkdir(fs, path, mode)          (fs)->i->mkdir(fs, path, mode)
#define ss_vfsrmdir(fs, path)                (fs)->i->rmdir(fs, path)
#define ss_vfsopen(fs, path, flags, mode)    (fs)->i->open(fs, path, flags, mode)
#define ss_vfsclose(fs, fd)                  (fs)->i->close(fs, fd)
#define ss_vfssync(fs, fd)                   (fs)->i->sync(fs, fd)
#define ss_vfsadvise(fs, fd, hint, off, len) (fs)->i->advise(fs, fd, hint, off, len)
#define ss_vfstruncate(fs, fd, size)         (fs)->i->truncate(fs, fd, size)
#define ss_vfspread(fs, fd, off, buf, size)  (fs)->i->pread(fs, fd, off, buf, size)
#define ss_vfspwrite(fs, fd, off, buf, size) (fs)->i->pwrite(fs, fd, off, buf, size)
#define ss_vfswrite(fs, fd, buf, size)       (fs)->i->write(fs, fd, buf, size)
#define ss_vfswritev(fs, fd, iov)            (fs)->i->writev(fs, fd, iov)
#define ss_vfsseek(fs, fd, off)              (fs)->i->seek(fs, fd, off)
#define ss_vfsmmap(fs, m, fd, size, ro)      (fs)->i->mmap(fs, m, fd, size, ro)
#define ss_vfsmmap_allocate(fs, m, size)     (fs)->i->mmap_allocate(fs, m, size)
#define ss_vfsmremap(fs, m, size)            (fs)->i->mremap(fs, m, size)
#define ss_vfsmunmap(fs, m)                  (fs)->i->munmap(fs, m)

#endif
