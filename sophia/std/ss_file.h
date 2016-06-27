#ifndef SS_FILE_H_
#define SS_FILE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct ssfile ssfile;

struct ssfile {
	int fd;
	uint64_t size;
	int creat;
	sspath path;
	ssvfs *vfs;
} sspacked;

static inline void
ss_fileinit(ssfile *f, ssvfs *vfs)
{
	ss_pathinit(&f->path);
	f->vfs   = vfs;
	f->fd    = -1;
	f->size  = 0;
	f->creat = 0;
}

static inline int
ss_fileopen_as(ssfile *f, char *path, int flags)
{
	f->creat = (flags & O_CREAT ? 1 : 0);
	f->fd = ss_vfsopen(f->vfs, path, flags, 0644);
	if (ssunlikely(f->fd == -1))
		return -1;
	ss_pathset(&f->path, "%s", path);
	f->size = 0;
	if (f->creat)
		return 0;
	int64_t size = ss_vfssize(f->vfs, path);
	if (ssunlikely(size == -1)) {
		ss_vfsclose(f->vfs, f->fd);
		f->fd = -1;
		return -1;
	}
	f->size = size;
	return 0;
}

static inline int
ss_fileopen(ssfile *f, char *path) {
	return ss_fileopen_as(f, path, O_RDWR);
}

static inline int
ss_filenew(ssfile *f, char *path) {
	return ss_fileopen_as(f, path, O_RDWR|O_CREAT);
}

static inline int
ss_fileclose(ssfile *f)
{
	if (ssunlikely(f->fd != -1)) {
		int rc = ss_vfsclose(f->vfs, f->fd);
		if (ssunlikely(rc == -1))
			return -1;
		f->fd  = -1;
		f->vfs = NULL;
	}
	return 0;
}

static inline int
ss_filerename(ssfile *f, char *path)
{
	int rc = ss_vfsrename(f->vfs, ss_pathof(&f->path), path);
	if (ssunlikely(rc == -1))
		return -1;
	ss_pathset(&f->path, "%s", path);
	return 0;
}

static inline int
ss_filesync(ssfile *f) {
	return ss_vfssync(f->vfs, f->fd);
}

static inline int
ss_filesync_range(ssfile *f, uint64_t off, uint64_t size) {
	return ss_vfssync_file_range(f->vfs, f->fd, off, size);
}

static inline int
ss_fileadvise(ssfile *f, int hint, uint64_t off, uint64_t len) {
	return ss_vfsadvise(f->vfs, f->fd, hint, off, len);
}

static inline int
ss_fileresize(ssfile *f, uint64_t size)
{
	int rc = ss_vfstruncate(f->vfs, f->fd, size);
	if (ssunlikely(rc == -1))
		return -1;
	f->size = size;
	return 0;
}

static inline int
ss_filepread(ssfile *f, uint64_t off, void *buf, int size)
{
	int64_t rc = ss_vfspread(f->vfs, f->fd, off, buf, size);
	if (ssunlikely(rc == -1))
		return -1;
	assert(rc == size);
	return rc;
}

static inline int
ss_filepwrite(ssfile *f, uint64_t off, void *buf, int size)
{
	int64_t rc = ss_vfspwrite(f->vfs, f->fd, off, buf, size);
	if (ssunlikely(rc == -1))
		return -1;
	assert(rc == size);
	return rc;
}

static inline int
ss_filewrite(ssfile *f, void *buf, int size)
{
	int64_t rc = ss_vfswrite(f->vfs, f->fd, buf, size);
	if (ssunlikely(rc == -1))
		return -1;
	assert(rc == size);
	f->size += rc;
	return rc;
}

static inline int
ss_filewritev(ssfile *f, ssiov *iov)
{
	int64_t rc = ss_vfswritev(f->vfs, f->fd, iov);
	if (ssunlikely(rc == -1))
		return -1;
	f->size += rc;
	return rc;
}

static inline int
ss_fileseek(ssfile *f, uint64_t off)
{
	return ss_vfsseek(f->vfs, f->fd, off);
}

static inline uint64_t
ss_filesvp(ssfile *f) {
	return f->size;
}

static inline int
ss_filerlb(ssfile *f, uint64_t svp)
{
	if (ssunlikely(f->size == svp))
		return 0;
	int rc = ss_vfstruncate(f->vfs, f->fd, svp);
	if (ssunlikely(rc == -1))
		return -1;
	f->size = svp;
	rc = ss_fileseek(f, f->size);
	if (ssunlikely(rc == -1))
		return -1;
	return 0;
}

#endif
