
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sp.h>

void sp_vef(spe *e, int type, va_list args)
{
	sp_lock(&e->lock);
	if (e->type & SPEF) {
		sp_unlock(&e->lock);
		return;
	}
	const char *fmt;
	int len;
	uint32_t epoch;

	e->type = type;
	switch (type & ~SPEF) {
	case SPE: {
		fmt = va_arg(args, const char*);
		len = snprintf(e->e, sizeof(e->e), "error: ");
		vsnprintf(e->e + len, sizeof(e->e) - len, fmt, args);
		break;
	}
	case SPEOOM: {
		fmt = va_arg(args, const char*);
		len = snprintf(e->e, sizeof(e->e), "out-of-memory error: ");
		vsnprintf(e->e + len, sizeof(e->e) - len, fmt, args);
		break;
	}
	case SPESYS: {
		e->errno_ = errno;
		fmt  = va_arg(args, const char*);
		len  = snprintf(e->e, sizeof(e->e), "system error: ");
		len += vsnprintf(e->e + len, sizeof(e->e) - len, fmt, args);
		snprintf(e->e + len, sizeof(e->e) - len, " (errno: %d, %s)",
		         e->errno_, strerror(e->errno_));
		break;
	}
	case SPEIO: {
		e->errno_ = errno;
		epoch = va_arg(args, uint32_t);
		fmt  = va_arg(args, const char*);
		len  = snprintf(e->e, sizeof(e->e), "io error: [epoch %"PRIu32"] ", epoch);
		len += vsnprintf(e->e + len, sizeof(e->e) - len, fmt, args);
		snprintf(e->e + len, sizeof(e->e) - len, " (errno: %d, %s)",
		         e->errno_, strerror(e->errno_));
		break;
	}
	}
	sp_unlock(&e->lock);
}
