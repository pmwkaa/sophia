
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libsr.h>

void sr_triggerinit(srtrigger *t)
{
	t->func = NULL;
	t->arg = NULL;
}

void *sr_triggerpointer_of(char *name)
{
	if (strncmp(name, "pointer:", 8) != 0)
		return NULL;
	name += 8;
	errno = 0;
	char *end;
	unsigned long long int pointer = strtoull(name, &end, 16);
	if (pointer == 0 && end == name) {
		return NULL;
	} else
	if (pointer == ULLONG_MAX && errno) {
		return NULL;
	}
	return (void*)(uintptr_t)pointer;
}

int sr_triggerset(srtrigger *t, char *name)
{
	void *ptr = sr_triggerpointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	t->func = (srtriggerf)(uintptr_t)ptr;
	return 0;
}

int sr_triggersetarg(srtrigger *t, char *name)
{
	void *ptr = sr_triggerpointer_of(name);
	if (srunlikely(ptr == NULL))
		return -1;
	t->arg = ptr;
	return 0;
}
