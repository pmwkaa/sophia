#ifndef SR_TRIGGER_H_
#define SR_TRIGGER_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef int (*srtriggerf)(void *object, void *arg);

typedef struct srtrigger srtrigger;

struct srtrigger {
	srtriggerf func;
	void *arg;
};

void *sr_triggerpointer_of(char*);
void  sr_triggerinit(srtrigger*);
int   sr_triggerset(srtrigger*, char*);
int   sr_triggersetarg(srtrigger*, char*);

static inline void
sr_triggerrun(srtrigger *t, void *object)
{
	if (t->func == NULL)
		return;
	t->func(object, t->arg);
}

#endif
