#ifndef SW_CONF_H_
#define SW_CONF_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct swconf swconf;

struct swconf {
	uint32_t  enable;
	char     *path;
	uint32_t  sync_on_rotate;
	uint32_t  sync_on_write;
	uint32_t  rotatewm;
};

void sw_confinit(swconf*);
void sw_conffree(swconf*, ssa*);
int  sw_confset_path(swconf*, ssa*, char*);

#endif
