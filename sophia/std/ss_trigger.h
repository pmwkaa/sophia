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
	sstriggerf function;
	void *arg;
};

static inline void
ss_triggerinit(sstrigger *t)
{
	t->function = NULL;
	t->arg = NULL;
}

static inline void
ss_triggerset(sstrigger *t, void *pointer)
{
	t->function = (sstriggerf)(uintptr_t)pointer;
}

static inline void
ss_triggerset_arg(sstrigger *t, void *pointer)
{
	t->arg = pointer;
}

static inline void
ss_triggerrun(sstrigger *t)
{
	if (t->function == NULL)
		return;
	t->function(t->arg);
}

#endif
