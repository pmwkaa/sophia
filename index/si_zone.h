#ifndef SI_ZONE_H_
#define SI_ZONE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sizone sizone;
typedef struct sizonemap sizonemap;

struct sizone {
	uint32_t enable;
	char     name[4];
	uint32_t mode;
	uint32_t compact_wm;
	uint32_t branch_prio;
	uint32_t branch_wm;
	uint32_t branch_ttl;
	uint32_t branch_ttl_wm;
	uint32_t backup_prio;
	uint32_t gc_prio;
	uint32_t gc_period;
	uint32_t gc_wm;
};

struct sizonemap {
	sizone zones[11];
};

static inline int
si_zonemap_init(sizonemap *m) {
	memset(m->zones, 0, sizeof(m->zones));
	return 0;
}

static inline void
si_zonemap_set(sizonemap *m, uint32_t percent, sizone *z)
{
	if (srunlikely(percent > 100))
		percent = 100;
	percent = percent - percent % 10;
	int p = percent / 10;
	m->zones[p] = *z;
	snprintf(m->zones[p].name, sizeof(m->zones[p].name), "%d", percent);
}

static inline sizone*
si_zonemap(sizonemap *m, uint32_t percent)
{
	if (srunlikely(percent > 100))
		percent = 100;
	percent = percent - percent % 10;
	int p = percent / 10;
	sizone *z = &m->zones[p];
	if (!z->enable) {
		while (p >= 0) {
			z = &m->zones[p];
			if (z->enable)
				return z;
			p--;
		}
		return NULL;
	}
	return z;
}

#endif
