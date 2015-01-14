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

#define SR_INJECTION_SD_BUILD_0      0
#define SR_INJECTION_SD_BUILD_1      1
#define SR_INJECTION_SI_BRANCH_0     2
#define SR_INJECTION_SI_COMPACTION_0 3
#define SR_INJECTION_SI_COMPACTION_1 4
#define SR_INJECTION_SI_COMPACTION_2 5
#define SR_INJECTION_SI_COMPACTION_3 6
#define SR_INJECTION_SI_COMPACTION_4 7
#define SR_INJECTION_SI_RECOVER_0    8

struct srinjection {
	int e[9];
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
