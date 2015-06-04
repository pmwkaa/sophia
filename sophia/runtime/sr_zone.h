#ifndef SR_ZONE_H_
#define SR_ZONE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct srzone srzone;
typedef struct srzonemap srzonemap;

struct srzone {
	uint32_t enable;
	char     name[4];
	uint32_t mode;
	uint32_t compact_wm;
	uint32_t branch_prio;
	uint32_t branch_wm;
	uint32_t branch_age;
	uint32_t branch_age_period;
	uint32_t branch_age_wm;
	uint32_t backup_prio;
	uint32_t gc_db_prio;
	uint32_t gc_prio;
	uint32_t gc_period;
	uint32_t gc_wm;
	uint32_t async;
};

struct srzonemap {
	srzone zones[11];
};

static inline int
sr_zonemap_init(srzonemap *m) {
	memset(m->zones, 0, sizeof(m->zones));
	return 0;
}

static inline void
sr_zonemap_set(srzonemap *m, uint32_t percent, srzone *z)
{
	if (ssunlikely(percent > 100))
		percent = 100;
	percent = percent - percent % 10;
	int p = percent / 10;
	m->zones[p] = *z;
	snprintf(m->zones[p].name, sizeof(m->zones[p].name), "%d", percent);
}

static inline srzone*
sr_zonemap(srzonemap *m, uint32_t percent)
{
	if (ssunlikely(percent > 100))
		percent = 100;
	percent = percent - percent % 10;
	int p = percent / 10;
	srzone *z = &m->zones[p];
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
