
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

int ss_fileunlink(char *path)
{
	return unlink(path);
}

int ss_filemove(char *a, char *b)
{
	return rename(a, b);
}

int ss_fileexists(char *path)
{
	struct stat st;
	int rc = lstat(path, &st);
	return rc == 0;
}

ssize_t ss_filesize(char *path)
{
	struct stat st;
	int rc = lstat(path, &st);
	if (ssunlikely(rc == -1))
		return -1;
	return st.st_size;
}

int ss_filemkdir(char *path)
{
	return mkdir(path, 0750);
}

static inline int
ss_fileopenas(ssfile *f, char *path, int flags)
{
	f->creat = (flags & O_CREAT ? 1 : 0);
	f->fd = open(path, flags, 0644);
	if (ssunlikely(f->fd == -1))
		return -1;
	f->file = ss_strdup(f->a, path);
	if (ssunlikely(f->file == NULL))
		goto err;
	f->size = 0;
	if (f->creat)
		return 0;
	struct stat st;
	int rc = lstat(path, &st);
	if (ssunlikely(rc == -1))
		goto err;
	f->size = st.st_size;
	return 0;
err:
	if (f->file) {
		ss_free(f->a, f->file);
		f->file = NULL;
	}
	close(f->fd);
	f->fd = -1;
	return -1;
}

int ss_fileopen(ssfile *f, char *path)
{
	return ss_fileopenas(f, path, O_RDWR);
}

int ss_filenew(ssfile *f, char *path)
{
	return ss_fileopenas(f, path, O_RDWR|O_CREAT);
}

int ss_filerename(ssfile *f, char *path)
{
	char *p = ss_strdup(f->a, path);
	if (ssunlikely(p == NULL))
		return -1;
	int rc = ss_filemove(f->file, p);
	if (ssunlikely(rc == -1)) {
		ss_free(f->a, p);
		return -1;
	}
	ss_free(f->a, f->file);
	f->file = p;
	return 0;
}

int ss_fileclose(ssfile *f)
{
	if (sslikely(f->file)) {
		ss_free(f->a, f->file);
		f->file = NULL;
	}
	int rc;
	if (ssunlikely(f->fd != -1)) {
		rc = close(f->fd);
		if (ssunlikely(rc == -1))
			return -1;
		f->fd = -1;
	}
	return 0;
}

int ss_filesync(ssfile *f)
{
#if defined(__APPLE__)
	return fcntl(f->fd, F_FULLFSYNC);
#elif defined(__FreeBSD__) || defined(__DragonFly__)
	return fsync(f->fd);
#else
    // at least Linux, OpenBSD and NetBSD have fdatasync().
	return fdatasync(f->fd);
#endif
}

int ss_fileresize(ssfile *f, uint64_t size)
{
	int rc = ftruncate(f->fd, size);
	if (ssunlikely(rc == -1))
		return -1;
	f->size = size;
	return 0;
}

int ss_filepread(ssfile *f, uint64_t off, void *buf, size_t size)
{
	size_t n = 0;
	do {
		ssize_t r;
		do {
			r = pread(f->fd, (char*)buf + n, size - n, off + n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);

	return 0;
}

int ss_filewrite(ssfile *f, void *buf, size_t size)
{
	size_t n = 0;
	do {
		ssize_t r;
		do {
			r = write(f->fd, (char*)buf + n, size - n);
		} while (r == -1 && errno == EINTR);
		if (r <= 0)
			return -1;
		n += r;
	} while (n != size);
	f->size += size;
	return 0;
}

int ss_filewritev(ssfile *f, ssiov *iv)
{
	struct iovec *v = iv->v;
	int n = iv->iovc;
	uint64_t size = 0;
	do {
		int r;
		do {
			r = writev(f->fd, v, n);
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
	f->size += size;
	return 0;
}

int ss_fileseek(ssfile *f, uint64_t off)
{
	return lseek(f->fd, off, SEEK_SET);
}

int ss_filerlb(ssfile *f, uint64_t svp)
{
	if (ssunlikely(f->size == svp))
		return 0;
	int rc = ftruncate(f->fd, svp);
	if (ssunlikely(rc == -1))
		return -1;
	f->size = svp;
	rc = ss_fileseek(f, f->size);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}
