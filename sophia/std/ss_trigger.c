
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>

void ss_triggerinit(sstrigger *t)
{
	t->func = NULL;
	t->arg = NULL;
}

void *ss_triggerpointer_of(char *name)
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

int ss_triggerset(sstrigger *t, char *name)
{
	void *ptr = ss_triggerpointer_of(name);
	if (ssunlikely(ptr == NULL))
		return -1;
	t->func = (sstriggerf)(uintptr_t)ptr;
	return 0;
}

int ss_triggersetarg(sstrigger *t, char *name)
{
	void *ptr = ss_triggerpointer_of(name);
	if (ssunlikely(ptr == NULL))
		return -1;
	t->arg = ptr;
	return 0;
}
