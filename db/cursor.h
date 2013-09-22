#ifndef SP_CURSOR_H_
#define SP_CURSOR_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct spc spc;

#define SPCNONE  0
#define SPCITXN  1
#define SPCI0    2
#define SPCI1    4
#define SPCP    16

struct spc {
	spmagic m;
	sporder o;
	sp *s;
	spii i0, i1;
	spii itxn;
	sppageh *ph;
	sppage *p;
	int pi;      /* page space index */
	spvh *pv;
	int pvi;     /* version page index */
	int mask;    /* last iteration advance mask */
	spref r;     /* last iteration result */
};

void sp_cursoropen(spc*, sp*, sporder, char*, int);
void sp_cursorclose(spc*);

int sp_iterate(spc*);
int sp_match(sp*, void*, size_t, void**, size_t*);

#endif
