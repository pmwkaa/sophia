#ifndef SR_INJECTION_H_
#define SR_INJECTION_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srinjection srinjection;

#define SR_INJECTION_SI_BRANCH_0 0
#define SR_INJECTION_SI_BRANCH_1 1
#define SR_INJECTION_SI_MERGE_0  2
#define SR_INJECTION_SI_MERGE_1  3
#define SR_INJECTION_SI_MERGE_2  4
#define SR_INJECTION_SI_MERGE_3  5
#define SR_INJECTION_SI_MERGE_4  6

struct srinjection {
	int e[7];
};

#ifdef SR_INJECTION_ENABLE
	#define SR_INJECTION(E, ID, X) \
	if ((E)->e[(ID)]) { \
		X; \
	} else {}
#else
	#define SR_INJECTION(E, ID, X)
#endif

#endif
