#ifndef SS_TYPE_H_
#define SS_TYPE_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef enum {
	SS_UNDEF,
	SS_STRING,
	SS_U32,
	SS_U64,
	SS_I64,
	SS_OBJECT,
	SS_FUNCTION
} sstype;

static inline char*
ss_typeof(sstype type) {
	switch (type) {
	case SS_UNDEF:    return "undef";
	case SS_STRING:   return "string";
	case SS_U32:      return "u32";
	case SS_U64:      return "u64";
	case SS_I64:      return "i64";
	case SS_OBJECT:   return "object";
	case SS_FUNCTION: return "function";
	}
	return NULL;
}

#endif
