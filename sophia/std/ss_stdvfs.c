
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

static inline int
ss_stdvfs_init(ssvfs *f ssunused, va_list args ssunused)
{
	return 0;
}

static inline void
ss_stdvfs_free(ssvfs *f ssunused)
{ }

static int64_t
ss_stdvfs_size(ssvfs *f ssunused, char *path)
{
	struct stat st;
	int rc = lstat(path, &st);
	if (ssunlikely(rc == -1))
		return -1;
	return st.st_size;
}

static int
ss_stdvfs_exists(ssvfs *f ssunused, char *path)
{
	struct stat st;
	int rc = lstat(path, &st);
	return rc == 0;
}

static int
ss_stdvfs_unlink(ssvfs *f ssunused, char *path)
{
	return unlink(path);
}

static int
ss_stdvfs_rename(ssvfs *f ssunused, char *src, char *dest)
{
	return rename(src, dest);
}

static int
ss_stdvfs_mkdir(ssvfs *f ssunused, char *path, int mode)
{
	return mkdir(path, mode);
}

static int
ss_stdvfs_rmdir(ssvfs *f ssunused, char *path)
{
	return rmdir(path);
}

static int
ss_stdvfs_open(ssvfs *f ssunused, char *path, int flags, int mode)
{
	return open(path, flags, mode);
}

static int
ss_stdvfs_close(ssvfs *f ssunused, int fd)
{
	return close(fd);
}

static int
ss_stdvfs_sync(ssvfs *f ssunused, int fd)
{
#if defined(__APPLE__)
	return fcntl(fd, F_FULLFSYNC);
#elif defined(__FreeBSD__) || defined(__DragonFly__)
	return fsync(fd);
#else
	return fdatasync(fd);
#endif
}

static int
ss_stdvfs_sync_file_range(ssvfs *f ssunused, int fd, uint64_t off, uint64_t size)
{
	int rc;
#ifdef OS_LINUX
	rc = sync_file_range(fd, SYNC_FILE_RANGE_WRITE|SYNC_FILE_RANGE_WAIT_AFTER,
	                     start, size);
#else
	rc = ss_stdvfs_sync(f, fd);
	(void)off;
	(void)size;
#endif
	return rc;
}

static int
ss_stdvfs_advise(ssvfs *f ssunused, int fd, int hint, uint64_t off, uint64_t len)
{
	(void)hint;
#if  defined(__APPLE__) || \
     defined(__FreeBSD__) || \
    (defined(__FreeBSD_kernel__) && defined(__GLIBC__)) || \
     defined(__DragonFly__)
	(void)fd;
	(void)off;
	(void)len;
	return 0;
#else
	return posix_fadvise(fd, off, len, POSIX_FADV_DONTNEED);
#endif
}

static int
ss_stdvfs_truncate(ssvfs *f ssunused, int fd, uint64_t size)
{
	return ftruncate(fd, size);
}

static int64_t
ss_stdvfs_pread(ssvfs *f ssunused, int fd, uint64_t off, void *buf, int size)
{
	int n = 0;
	do {
		int r;
		do {
			r = pread(fd, (char*)buf + n, size - n, off + n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

static int64_t
ss_stdvfs_pwrite(ssvfs *f ssunused, int fd, uint64_t off, void *buf, int size)
{
	int n = 0;
	do {
		int r;
		do {
			r = pwrite(fd, (char*)buf + n, size - n, off + n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

static int64_t
ss_stdvfs_write(ssvfs *f ssunused, int fd, void *buf, int size)
{
	int n = 0;
	do {
		int r;
		do {
			r = write(fd, (char*)buf + n, size - n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return n;
}

static int64_t
ss_stdvfs_writev(ssvfs *f ssunused, int fd, ssiov *iov)
{
	struct iovec *v = iov->v;
	int n = iov->iovc;
	int size = 0;
	do {
		int r;
		do {
			r = writev(fd, v, n);
		} while (r == -1 && errno == EINTR);
		if (r < 0)
			return -1;
		size += r;
		while (n > 0) {
			if (v->iov_len > (size_t)r) {
				v->iov_base = (char*)v->iov_base + r;
				v->iov_len -= r;
				break;
			} else {
				r -= v->iov_len;
				v++;
				n--;
			}
		}
	} while (n > 0);

	return size;
}

static int64_t
ss_stdvfs_seek(ssvfs *f ssunused, int fd, uint64_t off)
{
	return lseek(fd, off, SEEK_SET);
}

static int
ss_stdvfs_ioprio_low(ssvfs *f ssunused)
{
	int rc = 0;
#ifdef OS_LINUX
	/* set lowest io priority (idle) to a calling thread */
#define _IOPRIO_WHO_PROCESS 1
#define _IOPRIO_CLASS_SHIFT 13
#define _IOPRIO_PRIO_VALUE(class, data) \
	(((class) << _IOPRIO_CLASS_SHIFT) | data)
	rc = syscall(SYS_ioprio_set, _IOPRIO_WHO_PROCESS, 0,
                 _IOPRIO_PRIO_VALUE(3, 0));
#undef _IOPRIO_WHO_PROCESS
#undef _IOPRIO_CLASS_SHIFT
#undef _IOPRIO_PRIO_VALUE
#endif
	return rc;
}

static int
ss_stdvfs_mmap(ssvfs *f ssunused, ssmmap *m, int fd, uint64_t size, int ro)
{
	int flags = PROT_READ;
	if (! ro)
		flags |= PROT_WRITE;
	m->p = mmap(NULL, size, flags, MAP_SHARED, fd, 0);
	if (m->p == MAP_FAILED) {
		m->p = NULL;
		return -1;
	}
	m->size = size;
	return 0;
}

static int
ss_stdvfs_mmap_allocate(ssvfs *f ssunused, ssmmap *m, uint64_t size)
{
	int flags = PROT_READ|PROT_WRITE;
	m->p = mmap(NULL, size, flags, MAP_PRIVATE|MAP_ANON, -1, 0);
	if (ssunlikely(m->p == MAP_FAILED)) {
		m->p = NULL;
		return -1;
	}
	m->size = size;
	return 0;
}

static int
ss_stdvfs_mremap(ssvfs *f ssunused, ssmmap *m, uint64_t size)
{
	if (ssunlikely(m->p == NULL))
		return ss_stdvfs_mmap_allocate(f, m, size);
	void *p;
#if  defined(__APPLE__) || \
     defined(__FreeBSD__) || \
    (defined(__FreeBSD_kernel__) && defined(__GLIBC__)) || \
     defined(__DragonFly__)
	p = mmap(NULL, size, PROT_READ|PROT_WRITE,
	         MAP_PRIVATE|MAP_ANON, -1, 0);
	if (p == MAP_FAILED)
		return -1;
	uint64_t to_copy = m->size;
	if (to_copy > size)
		to_copy = size;
	memcpy(p, m->p, size);
	munmap(m->p, m->size);
#else
	p = mremap(m->p, m->size, size, MREMAP_MAYMOVE);
	if (ssunlikely(p == MAP_FAILED))
		return -1;
#endif
	m->p = p;
	m->size = size;
	return 0;
}

static int
ss_stdvfs_munmap(ssvfs *f ssunused, ssmmap *m)
{
	if (ssunlikely(m->p == NULL))
		return 0;
	int rc = munmap(m->p, m->size);
	m->p = NULL;
	return rc;
}

ssvfsif ss_stdvfs =
{
	.init            = ss_stdvfs_init,
	.free            = ss_stdvfs_free,
	.size            = ss_stdvfs_size,
	.exists          = ss_stdvfs_exists,
	.unlink          = ss_stdvfs_unlink,
	.rename          = ss_stdvfs_rename,
	.mkdir           = ss_stdvfs_mkdir,
	.rmdir           = ss_stdvfs_rmdir,
	.open            = ss_stdvfs_open,
	.close           = ss_stdvfs_close,
	.sync            = ss_stdvfs_sync,
	.sync_file_range = ss_stdvfs_sync_file_range,
	.advise          = ss_stdvfs_advise,
	.truncate        = ss_stdvfs_truncate,
	.pread           = ss_stdvfs_pread,
	.pwrite          = ss_stdvfs_pwrite,
	.write           = ss_stdvfs_write,
	.writev          = ss_stdvfs_writev,
	.seek            = ss_stdvfs_seek,
	.ioprio_low      = ss_stdvfs_ioprio_low,
	.mmap            = ss_stdvfs_mmap,
	.mmap_allocate   = ss_stdvfs_mmap_allocate,
	.mremap          = ss_stdvfs_mremap,
	.munmap          = ss_stdvfs_munmap
};
