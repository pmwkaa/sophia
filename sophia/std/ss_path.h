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
ss_pathset(sspath *p, char *fmt, ...)
{
	va_list args;
	va_start(args, fmt);
	vsnprintf(p->path, sizeof(p->path), fmt, args);
	va_end(args);
}

static inline void
ss_pathA(sspath *p, char *dir, uint32_t id, char *ext)
{
	ss_pathset(p, "%s/%010"PRIu32"%s", dir, id, ext);
}

static inline void
ss_pathAB(sspath *p, char *dir, uint32_t a, uint32_t b, char *ext)
{
	ss_pathset(p, "%s/%010"PRIu32".%010"PRIu32"%s", dir, a, b, ext);
}

#endif
