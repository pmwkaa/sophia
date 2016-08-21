
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libsw.h>

static inline ssize_t sw_diridof(char *s)
{
	size_t v = 0;
	while (*s && *s != '.') {
		if (ssunlikely(!isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	return v;
}

static inline swdirid*
sw_dirmatch(ssbuf *list, uint64_t id)
{
	if (ssunlikely(ss_bufused(list) == 0))
		return NULL;
	swdirid *n = (swdirid*)list->s;
	while ((char*)n < list->p) {
		if (n->id == id)
			return n;
		n++;
	}
	return NULL;
}

static inline swdirtype*
sw_dirtypeof(swdirtype *types, char *ext)
{
	swdirtype *p = &types[0];
	int n = 0;
	while (p[n].ext != NULL) {
		if (strcmp(p[n].ext, ext) == 0)
			return &p[n];
		n++;
	}
	return NULL;
}

static int
sw_dircmp(const void *p1, const void *p2)
{
	swdirid *a = (swdirid*)p1;
	swdirid *b = (swdirid*)p2;
	assert(a->id != b->id);
	return (a->id > b->id)? 1: -1;
}

int sw_dirread(ssbuf *list, ssa *a, swdirtype *types, char *dir)
{
	DIR *d = opendir(dir);
	if (ssunlikely(d == NULL))
		return -1;

	struct dirent *de;
	while ((de = readdir(d))) {
		if (ssunlikely(de->d_name[0] == '.'))
			continue;
		ssize_t id = sw_diridof(de->d_name);
		if (ssunlikely(id == -1))
			goto error;
		char *ext = strstr(de->d_name, ".");
		if (ssunlikely(ext == NULL))
			goto error;
		ext++;
		swdirtype *type = sw_dirtypeof(types, ext);
		if (ssunlikely(type == NULL))
			continue;
		swdirid *n = sw_dirmatch(list, id);
		if (n) {
			n->mask |= type->mask;
			type->count++;
			continue;
		}
		int rc = ss_bufensure(list, a, sizeof(swdirid));
		if (ssunlikely(rc == -1))
			goto error;
		n = (swdirid*)list->p;
		ss_bufadvance(list, sizeof(swdirid));
		n->id  = id;
		n->mask = type->mask;
		type->count++;
	}
	closedir(d);

	if (ssunlikely(ss_bufused(list) == 0))
		return 0;

	int n = ss_bufused(list) / sizeof(swdirid);
	qsort(list->s, n, sizeof(swdirid), sw_dircmp);
	return n;

error:
	closedir(d);
	return -1;
}
