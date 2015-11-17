#ifndef SS_PATH_H_
#define SS_PATH_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sspath sspath;

struct sspath {
	char path[PATH_MAX];
};

static inline void
ss_pathinit(sspath *p)
{
	p->path[0] = 0;
}

static inline void
ss_pathset(sspath *p, char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(p->path, sizeof(p->path), fmt, args);
	va_end(args);
}

static inline void
ss_path32(sspath *p, char *dir, uint32_t id, char *ext)
{
	ss_pathset(p, "%s/%010"PRIu32"%s", dir, id, ext);
}

static inline void
ss_path64(sspath *p, char *dir, uint64_t id, char *ext)
{
	ss_pathset(p, "%s/%020"PRIu64"%s", dir, id, ext);
}

static inline void
ss_path64_compound(sspath *p, char *dir, uint64_t a, uint64_t b, char *ext)
{
	ss_pathset(p, "%s/%020"PRIu64".%020"PRIu64"%s", dir, a, b, ext);
}

static inline char*
ss_pathof(sspath *p) {
	return p->path;
}

static inline int
ss_pathis_set(sspath *p) {
	return p->path[0] != 0;
}

#endif
