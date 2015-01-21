#ifndef SR_FILE_H_
#define SR_FILE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srfile srfile;

struct srfile {
	sra *a;
	int creat;
	uint64_t size;
	char *file;
	int fd;
};

static inline void
sr_fileinit(srfile *f, sra *a)
{
	memset(f, 0, sizeof(*f));
	f->a = a;
	f->fd = -1;
}

static inline uint64_t
sr_filesvp(srfile *f) {
	return f->size;
}

int sr_fileunlink(char*);
int sr_filemove(char*, char*);
int sr_fileexists(char*);
int sr_filemkdir(char*);
int sr_fileopen(srfile*, char*);
int sr_filenew(srfile*, char*);
int sr_filerename(srfile*, char*);
int sr_fileclose(srfile*);
int sr_filesync(srfile*);
int sr_fileresize(srfile*, uint64_t);
int sr_filepread(srfile*, uint64_t, void*, size_t);
int sr_filewrite(srfile*, void*, size_t);
int sr_filewritev(srfile*, sriov*);
int sr_fileseek(srfile*, uint64_t);
int sr_filelock(srfile*);
int sr_fileunlock(srfile*);
int sr_filerlb(srfile*, uint64_t);

#endif
