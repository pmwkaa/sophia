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
	ssa *a;
	int creat;
	uint64_t size;
	char *file;
	int fd;
};

static inline void
ss_fileinit(ssfile *f, ssa *a)
{
	f->a     = a;
	f->creat = 0;
	f->size  = 0;
	f->file  = NULL;
	f->fd    = -1;
}

static inline uint64_t
ss_filesvp(ssfile *f) {
	return f->size;
}

ssize_t ss_filesize(char*);
int ss_fileunlink(char*);
int ss_filemove(char*, char*);
int ss_fileexists(char*);
int ss_filemkdir(char*);
int ss_fileopen(ssfile*, char*);
int ss_filenew(ssfile*, char*);
int ss_filerename(ssfile*, char*);
int ss_fileclose(ssfile*);
int ss_filesync(ssfile*);
int ss_fileresize(ssfile*, uint64_t);
int ss_filepread(ssfile*, uint64_t, void*, size_t);
int ss_filewrite(ssfile*, void*, size_t);
int ss_filewritev(ssfile*, ssiov*);
int ss_fileseek(ssfile*, uint64_t);
int ss_filelock(ssfile*);
int ss_fileunlock(ssfile*);
int ss_filerlb(ssfile*, uint64_t);

#endif
