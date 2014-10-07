
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

int sr_fileunlink(char *path)
{
	return unlink(path);
}

int sr_fileexists(char *path)
{
	struct stat st;
	int rc = lstat(path, &st);
	return rc == 0;
}

int sr_filemkdir(char *path)
{
	return mkdir(path, 0750);
}

static inline int
sr_fileopenas(srfile *f, char *path, int flags)
{
	f->creat = (flags & O_CREAT ? 1 : 0);
	f->fd = open(path, flags, 0644);
	if (srunlikely(f->fd == -1))
		return -1;
	f->file = sr_strdup(f->a, path);
	if (srunlikely(f->file == NULL))
		goto err;
	f->size = 0;
	if (f->creat)
		return 0;
	struct stat st;
	int rc = lstat(path, &st);
	if (srunlikely(rc == -1))
		goto err;
	f->size = st.st_size;
	return 0;
err:
	if (f->file) {
		sr_free(f->a, f->file);
		f->file = NULL;
	}
	close(f->fd);
	f->fd = -1;
	return -1;
}

int sr_fileopen(srfile *f, char *path)
{
	return sr_fileopenas(f, path, O_RDWR);
}

int sr_filenew(srfile *f, char *path)
{
	return sr_fileopenas(f, path, O_RDWR|O_CREAT);
}

int sr_filerename(srfile *f, char *path)
{
	char *p = sr_strdup(f->a, path);
	if (srunlikely(p == NULL))
		return -1;
	int rc = rename(f->file, p);
	if (srunlikely(rc == -1)) {
		sr_free(f->a, p);
		return -1;
	}
	sr_free(f->a, f->file);
	f->file = p;
	return 0;
}

int sr_fileclose(srfile *f)
{
	if (srlikely(f->file)) {
		sr_free(f->a, f->file);
		f->file = NULL;
	}
	int rc;
	if (srunlikely(f->fd != -1)) {
		rc = close(f->fd);
		if (srunlikely(rc == -1))
			return -1;
		f->fd = -1;
	}
	return 0;
}

int sr_filesync(srfile *f) {
	return fdatasync(f->fd);
}

int sr_fileresize(srfile *f, uint64_t size)
{
	int rc = ftruncate(f->fd, size);
	if (srunlikely(rc == -1))
		return -1;
	f->size = size;
	return 0;
}

int sr_filepread(srfile *f, uint64_t off, void *buf, size_t size)
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

int sr_filewrite(srfile *f, void *buf, size_t size)
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

int sr_filewritev(srfile *f, sriov *iv)
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

int sr_fileseek(srfile *f, uint64_t off)
{
	return lseek(f->fd, off, SEEK_SET);
}

int sr_filelock(srfile *f)
{
	struct flock l;
	memset(&l, 0, sizeof(l));
	l.l_whence = SEEK_SET;
	l.l_start = 0;
	l.l_len = 0;
	l.l_type = F_WRLCK;
	return fcntl(f->fd, F_SETLK, &l);
}

int sr_fileunlock(srfile *f)
{
	if (srunlikely(f->fd == -1))
		return 0;
	struct flock l;
	memset(&l, 0, sizeof(l));
	l.l_whence = SEEK_SET;
	l.l_start = 0;
	l.l_len = 0;
	l.l_type = F_UNLCK;
	return fcntl(f->fd, F_SETLK, &l);
}

int sr_filerlb(srfile *f, uint64_t svp)
{
	if (srunlikely(f->size == svp))
		return 0;
	int rc = ftruncate(f->fd, svp);
	if (srunlikely(rc == -1))
		return -1;
	f->size = svp;
	return lseek(f->fd, f->size, SEEK_SET);
}
