#ifndef SS_TRIGGER_H_
#define SS_TRIGGER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef int (*sstriggerf)(void *arg);

typedef struct sstrigger sstrigger;

struct sstrigger {
	sstriggerf func;
	void *arg;
};

void *ss_triggerpointer_of(char*);
void  ss_triggerinit(sstrigger*);
int   ss_triggerset(sstrigger*, char*);
int   ss_triggersetarg(sstrigger*, char*);

static inline void
ss_triggerrun(sstrigger *t)
{
	if (t->func == NULL)
		return;
	t->func(t->arg);
}

#endif
