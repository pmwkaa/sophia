#ifndef SF_LIMIT_H_
#define SF_LIMIT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

typedef struct sflimit sflimit;

struct sflimit {
	uint8_t   u8_min;
	uint8_t   u8_max;
	uint16_t  u16_min;
	uint16_t  u16_max;
	uint32_t  u32_min;
	uint32_t  u32_max;
	uint64_t  u64_min;
	uint64_t  u64_max;
	int64_t   i64_min;
	int64_t   i64_max;
	char     *string_min;
	uint32_t  string_min_size;
	char     *string_max;
	uint32_t  string_max_size;
	uint32_t  field_max_size;
};

static inline int
sf_limitinit(sflimit *b, ssa *a)
{
	b->field_max_size = 2 * 1024 * 1024;
	b->u8_min  = 0;
	b->u8_max  = UINT8_MAX;
	b->u16_min = 0;
	b->u16_max = UINT16_MAX;
	b->u32_min = 0;
	b->u32_max = UINT32_MAX;
	b->u64_min = 0;
	b->u64_max = UINT64_MAX;
	b->i64_min = INT64_MIN;
	b->i64_max = UINT64_MAX;
	b->string_min_size = 0;
	b->string_min = "";
	b->string_max_size = 1024;
	b->string_max = ss_malloc(a, b->string_max_size);
	if (ssunlikely(b->string_max == NULL))
		return -1;
	memset(b->string_max, 0xff, b->string_max_size);
	return 0;
}

static inline void
sf_limitfree(sflimit *b, ssa *a)
{
	if (b->string_max)
		ss_free(a, b->string_max);
}

static inline void
sf_limitapply(sflimit *b, sfscheme *s, sfv *fields, ssorder order)
{
	int i;
	for (i = 0; i < s->fields_count; i++) {
		sfv *v = &fields[i];
		if (v->pointer)
			continue;
		sffield *part = s->fields[i];
		switch (part->type) {
		case SS_U8:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u8_max;
				v->size = sizeof(uint8_t);
			} else {
				v->pointer = (char*)&b->u8_min;
				v->size = sizeof(uint8_t);
			}
			break;
		case SS_U8REV:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u8_min;
				v->size = sizeof(uint8_t);
			} else {
				v->pointer = (char*)&b->u8_max;
				v->size = sizeof(uint8_t);
			}
			break;
		case SS_U16:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u16_max;
				v->size = sizeof(uint16_t);
			} else {
				v->pointer = (char*)&b->u16_min;
				v->size = sizeof(uint16_t);
			}
			break;
		case SS_U16REV:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u16_min;
				v->size = sizeof(uint16_t);
			} else {
				v->pointer = (char*)&b->u16_max;
				v->size = sizeof(uint16_t);
			}
			break;
		case SS_U32:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u32_max;
				v->size = sizeof(uint32_t);
			} else {
				v->pointer = (char*)&b->u32_min;
				v->size = sizeof(uint32_t);
			}
			break;
		case SS_U32REV:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u32_min;
				v->size = sizeof(uint32_t);
			} else {
				v->pointer = (char*)&b->u32_max;
				v->size = sizeof(uint32_t);
			}
			break;
		case SS_U64:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u64_max;
				v->size = sizeof(uint64_t);
			} else {
				v->pointer = (char*)&b->u64_min;
				v->size = sizeof(uint64_t);
			}
			break;
		case SS_U64REV:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->u64_min;
				v->size = sizeof(uint64_t);
			} else {
				v->pointer = (char*)&b->u64_max;
				v->size = sizeof(uint64_t);
			}
			break;
		case SS_I64:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = (char*)&b->i64_max;
				v->size = sizeof(int64_t);
			} else {
				v->pointer = (char*)&b->i64_min;
				v->size = sizeof(int64_t);
			}
			break;
		case SS_STRING:
			if (order == SS_LT || order == SS_LTE) {
				v->pointer = b->string_max;
				v->size = b->string_max_size;
			} else {
				v->pointer = b->string_min;
				v->size = b->string_min_size;
			}
			break;
		default: assert(0);
			break;
		}
	}
}

#endif
