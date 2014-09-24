
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

static inline ssize_t sr_diridof(char *s)
{
	size_t v = 0;
	while (*s && *s != '.') {
		if (srunlikely(!isdigit(*s)))
			return -1;
		v = (v * 10) + *s - '0';
		s++;
	}
	return v;
}

static inline srdirid*
sr_dirmatch(srbuf *list, uint64_t id)
{
	if (srunlikely(sr_bufused(list) == 0))
		return NULL;
	srdirid *n = (srdirid*)list->s;
	while ((char*)n < list->p) {
		if (n->id == id)
			return n;
		n++;
	}
	return NULL;
}

static inline srdirtype*
sr_dirtypeof(srdirtype *types, char *ext)
{
	srdirtype *p = &types[0];
	int n = 0;
	while (p[n].ext != NULL) {
		if (strcmp(p[n].ext, ext) == 0)
			return &p[n];
		n++;
	}
	return NULL;
}

static int
sr_dircmp(const void *p1, const void *p2)
{
	srdirid *a = (srdirid*)p1;
	srdirid *b = (srdirid*)p2;
	assert(a->id != b->id);
	return (a->id > b->id)? 1: -1;
}

int sr_dirread(srbuf *list, sra *a, srdirtype *types, char *dir)
{
	DIR *d = opendir(dir);
	if (srunlikely(d == NULL))
		return -1;

	struct dirent *de;
	while ((de = readdir(d))) {
		if (srunlikely(de->d_name[0] == '.'))
			continue;
		ssize_t id = sr_diridof(de->d_name);
		if (srunlikely(id == -1))
			goto error;
		char *ext = strstr(de->d_name, ".");
		if (srunlikely(ext == NULL))
			goto error;
		ext++;
		srdirtype *type = sr_dirtypeof(types, ext);
		if (srunlikely(type == NULL))
			continue;
		srdirid *n = sr_dirmatch(list, id);
		if (n) {
			n->mask |= type->mask;
			type->count++;
			continue;
		}
		int rc = sr_bufensure(list, a, sizeof(srdirid));
		if (srunlikely(rc == -1))
			goto error;
		n = (srdirid*)list->p;
		sr_bufadvance(list, sizeof(srdirid));
		n->id  = id;
		n->mask = type->mask;
		type->count++;
	}
	closedir(d);

	if (srunlikely(sr_bufused(list) == 0))
		return 0;

	int n = sr_bufused(list) / sizeof(srdirid);
	qsort(list->s, n, sizeof(srdirid), sr_dircmp);
	return n;

error:
	closedir(d);
	return -1;
}
