#ifndef SL_CONF_H_
#define SL_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct slconf slconf;

struct slconf {
	uint32_t  enable;
	char     *path;
	uint32_t  sync_on_rotate;
	uint32_t  sync_on_write;
	uint32_t  rotatewm;
};

void sl_confinit(slconf*);
void sl_conffree(slconf*, ssa*);
int  sl_confset_path(slconf*, ssa*, char*);

#endif
