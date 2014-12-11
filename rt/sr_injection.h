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

#define SR_INJECTION_SI_BRANCH_0     0
#define SR_INJECTION_SI_BRANCH_1     1
#define SR_INJECTION_SI_COMPACTION_0 2
#define SR_INJECTION_SI_COMPACTION_1 3
#define SR_INJECTION_SI_COMPACTION_2 4
#define SR_INJECTION_SI_COMPACTION_3 5
#define SR_INJECTION_SI_COMPACTION_4 6
#define SR_INJECTION_SE_SNAPSHOT_0   7
#define SR_INJECTION_SE_SNAPSHOT_1   8
#define SR_INJECTION_SE_SNAPSHOT_2   9
#define SR_INJECTION_SE_SNAPSHOT_3   10

struct srinjection {
	int e[11];
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
