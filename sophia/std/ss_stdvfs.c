
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
    // at least Linux, OpenBSD and NetBSD have fdatasync().
	return fdatasync(fd);
#endif
}

static int
ss_stdvfs_advise(ssvfs *f ssunused, int fd, int hint, uint64_t off, uint64_t len)
{
	(void)hint;
	return posix_fadvise(fd, off, len, POSIX_FADV_DONTNEED);
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

ssvfsif ss_stdvfs =
{
	.init     = ss_stdvfs_init,
	.free     = ss_stdvfs_free,
	.size     = ss_stdvfs_size,
	.exists   = ss_stdvfs_exists,
	.unlink   = ss_stdvfs_unlink,
	.rename   = ss_stdvfs_rename,
	.mkdir    = ss_stdvfs_mkdir,
	.rmdir    = ss_stdvfs_rmdir,
	.open     = ss_stdvfs_open,
	.close    = ss_stdvfs_close,
	.sync     = ss_stdvfs_sync,
	.advise   = ss_stdvfs_advise,
	.truncate = ss_stdvfs_truncate,
	.pread    = ss_stdvfs_pread,
	.pwrite   = ss_stdvfs_pwrite,
	.write    = ss_stdvfs_write,
	.writev   = ss_stdvfs_writev,
	.seek     = ss_stdvfs_seek
};
