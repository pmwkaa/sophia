
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

static inline ssize_t ss_diridof(char *s)
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

static inline ssdirid*
ss_dirmatch(ssbuf *list, uint64_t id)
{
	if (ssunlikely(ss_bufused(list) == 0))
		return NULL;
	ssdirid *n = (ssdirid*)list->s;
	while ((char*)n < list->p) {
		if (n->id == id)
			return n;
		n++;
	}
	return NULL;
}

static inline ssdirtype*
ss_dirtypeof(ssdirtype *types, char *ext)
{
	ssdirtype *p = &types[0];
	int n = 0;
	while (p[n].ext != NULL) {
		if (strcmp(p[n].ext, ext) == 0)
			return &p[n];
		n++;
	}
	return NULL;
}

static int
ss_dircmp(const void *p1, const void *p2)
{
	ssdirid *a = (ssdirid*)p1;
	ssdirid *b = (ssdirid*)p2;
	assert(a->id != b->id);
	return (a->id > b->id)? 1: -1;
}

int ss_dirread(ssbuf *list, ssa *a, ssdirtype *types, char *dir)
{
	DIR *d = opendir(dir);
	if (ssunlikely(d == NULL))
		return -1;

	struct dirent *de;
	while ((de = readdir(d))) {
		if (ssunlikely(de->d_name[0] == '.'))
			continue;
		ssize_t id = ss_diridof(de->d_name);
		if (ssunlikely(id == -1))
			goto error;
		char *ext = strstr(de->d_name, ".");
		if (ssunlikely(ext == NULL))
			goto error;
		ext++;
		ssdirtype *type = ss_dirtypeof(types, ext);
		if (ssunlikely(type == NULL))
			continue;
		ssdirid *n = ss_dirmatch(list, id);
		if (n) {
			n->mask |= type->mask;
			type->count++;
			continue;
		}
		int rc = ss_bufensure(list, a, sizeof(ssdirid));
		if (ssunlikely(rc == -1))
			goto error;
		n = (ssdirid*)list->p;
		ss_bufadvance(list, sizeof(ssdirid));
		n->id  = id;
		n->mask = type->mask;
		type->count++;
	}
	closedir(d);

	if (ssunlikely(ss_bufused(list) == 0))
		return 0;

	int n = ss_bufused(list) / sizeof(ssdirid);
	qsort(list->s, n, sizeof(ssdirid), ss_dircmp);
	return n;

error:
	closedir(d);
	return -1;
}
