
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

typedef struct {
	ssspinlock lock;
	uint32_t fail_from;
	uint32_t n;
} sstestvfs;

static inline int
ss_testvfs_init(ssvfs *f, va_list args ssunused)
{
	sstestvfs *o = (sstestvfs*)f->priv;
	o->fail_from = va_arg(args, int);
	o->n = 0;
	ss_spinlockinit(&o->lock);
	return 0;
}

static inline void
ss_testvfs_free(ssvfs *f)
{
	sstestvfs *o = (sstestvfs*)f->priv;
	ss_spinlockfree(&o->lock);
}

static inline int
ss_testvfs_call(ssvfs *f)
{
	sstestvfs *o = (sstestvfs*)f->priv;
	ss_spinlock(&o->lock);
	int generate_fail = o->n >= o->fail_from;
	o->n++;
	ss_spinunlock(&o->lock);
	return generate_fail;
}

static int64_t
ss_testvfs_size(ssvfs *f, char *path)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.size(f, path);
}

static int
ss_testvfs_exists(ssvfs *f, char *path)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.exists(f, path);
}

static int
ss_testvfs_unlink(ssvfs *f, char *path)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.unlink(f, path);
}

static int
ss_testvfs_rename(ssvfs *f, char *src, char *dest)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.rename(f, src, dest);
}

static int
ss_testvfs_mkdir(ssvfs *f, char *path, int mode)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.mkdir(f, path, mode);
}

static int
ss_testvfs_rmdir(ssvfs *f, char *path)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.rmdir(f, path);
}

static int
ss_testvfs_open(ssvfs *f, char *path, int flags, int mode)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.open(f, path, flags, mode);
}

static int
ss_testvfs_close(ssvfs *f, int fd)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.close(f, fd);
}

static int
ss_testvfs_sync(ssvfs *f, int fd)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.sync(f, fd);
}

static int
ss_testvfs_sync_file_range(ssvfs *f ssunused, int fd, uint64_t start, uint64_t size)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.sync_file_range(f, fd, start, size);
}

static int
ss_testvfs_advise(ssvfs *f, int fd, int hint, uint64_t off, uint64_t len)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.advise(f, fd, hint, off, len);
}

static int
ss_testvfs_truncate(ssvfs *f, int fd, uint64_t size)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.truncate(f, fd, size);
}

static int64_t
ss_testvfs_pread(ssvfs *f, int fd, uint64_t off, void *buf, int size)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.pread(f, fd, off, buf, size);
}

static int64_t
ss_testvfs_write(ssvfs *f, int fd, void *buf, int size)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.write(f, fd, buf, size);
}

static int64_t
ss_testvfs_writev(ssvfs *f, int fd, ssiov *iov)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.writev(f, fd, iov);
}

static int64_t
ss_testvfs_seek(ssvfs *f, int fd, uint64_t off)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.seek(f, fd, off);
}

static int
ss_testvfs_ioprio_low(ssvfs *f)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.ioprio_low(f);
}

static int
ss_testvfs_mmap(ssvfs *f, ssmmap *m, int fd, uint64_t size, int ro)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.mmap(f, m, fd, size, ro);
}

static int
ss_testvfs_mmap_allocate(ssvfs *f, ssmmap *m, uint64_t size)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.mmap_allocate(f, m, size);
}

static int
ss_testvfs_mremap(ssvfs *f, ssmmap *m, uint64_t size)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.mremap(f, m, size);
}

static int
ss_testvfs_munmap(ssvfs *f, ssmmap *m)
{
	if (ss_testvfs_call(f))
		return -1;
	return ss_stdvfs.munmap(f, m);
}

ssvfsif ss_testvfs =
{
	.init            = ss_testvfs_init,
	.free            = ss_testvfs_free,
	.size            = ss_testvfs_size,
	.exists          = ss_testvfs_exists,
	.unlink          = ss_testvfs_unlink,
	.rename          = ss_testvfs_rename,
	.mkdir           = ss_testvfs_mkdir,
	.rmdir           = ss_testvfs_rmdir,
	.open            = ss_testvfs_open,
	.close           = ss_testvfs_close,
	.sync            = ss_testvfs_sync,
	.sync_file_range = ss_testvfs_sync_file_range,
	.advise          = ss_testvfs_advise,
	.truncate        = ss_testvfs_truncate,
	.pread           = ss_testvfs_pread,
	.write           = ss_testvfs_write,
	.writev          = ss_testvfs_writev,
	.seek            = ss_testvfs_seek,
	.ioprio_low      = ss_testvfs_ioprio_low,
	.mmap            = ss_testvfs_mmap,
	.mmap_allocate   = ss_testvfs_mmap_allocate,
	.mremap          = ss_testvfs_mremap,
	.munmap          = ss_testvfs_munmap
};
