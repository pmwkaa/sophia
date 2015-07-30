
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <sophia.h>
#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libsv.h>
#include <libso.h>
#include <libst.h>

static void
sr_meta_set_ns(void)
{
	srmeta root = {
		.key      = "test",
		.flags    = SR_NS,
		.type     = SS_UNDEF,
		.function = NULL,
		.value    = NULL,
		.ptr      = NULL,
		.next     = NULL
	};
	srmetastmt stmt = {
		.op        = SR_WRITE,
		.path      = "test",
		.value     = NULL,
		.valuetype = SS_UNDEF,
		.valuesize = 0,
		.serialize = NULL,
		.ptr       = NULL,
		.r         = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == -1 );
}

static int sr_meta_set_ns_trigger_called = 0;

static int
sr_meta_set_ns_trigger_f(srmeta *m, srmetastmt *s)
{
	assert(s->op == SR_WRITE);
	(void)m;
	sr_meta_set_ns_trigger_called = 1;
	return 0;
}

static void
sr_meta_set_ns_trigger(void)
{
	srmeta root = {
		.key      = "test",
		.flags    = SR_NS,
		.type     = SS_UNDEF,
		.function = sr_meta_set_ns_trigger_f,
		.value    = NULL,
		.ptr      = NULL,
		.next     = NULL
	};
	srmetastmt stmt = {
		.op = SR_WRITE,
		.path = "test",
		.value = NULL,
		.valuetype = SS_UNDEF,
		.valuesize = 0,
		.serialize = NULL,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( sr_meta_set_ns_trigger_called == 1 );
}

static int
sr_meta_set_u32_f(srmeta *c, srmetastmt *s)
{
	assert(s->op == SR_WRITE);
	sr_meta_write(c, s);
	return 0;
}

static void
sr_meta_set_u32(void)
{
	uint32_t value = 0;
	uint32_t set = 10;
	srmeta root = {
		.key = "test",
		.flags = 0,
		.type = SS_U32,
		.function = sr_meta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_WRITE,
		.path = "test",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 10 );
}

static void
sr_meta_set_ns_u32(void)
{
	uint32_t value = 0;
	uint32_t set = 15;
	srmeta u32 = {
		.key = "u32",
		.flags = 0,
		.type = SS_U32,
		.function = sr_meta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = sr_meta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_WRITE,
		.path = "test.u32",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 15 );
}

static void
sr_meta_set_ns_ns_u32_0(void)
{
	uint32_t value = 0;
	uint32_t set = 18;
	srmeta u32v = {
		.key = "u32",
		.flags = 0,
		.type = SS_U32,
		.function = sr_meta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta u32 = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = NULL,
		.value = &u32v,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = sr_meta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_WRITE,
		.path = "test.test.u32",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 18 );
}

static void
sr_meta_set_ns_ns_u32_1(void)
{
	uint32_t value = 0;
	uint64_t set = 18;
	srmeta u32v = {
		.key = "u32",
		.flags = 0,
		.type = SS_U32,
		.function = sr_meta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta u32 = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = NULL,
		.value = &u32v,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = sr_meta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_WRITE,
		.path = "test.test.u32",
		.value = &set,
		.valuetype = SS_U64,
		.valuesize = sizeof(uint64_t),
		.serialize = NULL,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 18 );
}

static void
sr_meta_set_ns_ns_u32_bad0(void)
{
	uint32_t value = 0;
	uint32_t set = 18;
	srmeta u32v = {
		.key = "u32",
		.flags = 0,
		.type = SS_U32,
		.function = sr_meta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta u32 = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = NULL,
		.value = &u32v,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.key = "test",
		.flags = SR_NS,
		.type = SS_UNDEF,
		.function = sr_meta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_WRITE,
		.path = "test.test.u64",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(&root, &stmt) == -1 );
	t( value == 0 );
}

static int
sr_meta_serialize_f(srmeta *c, srmetastmt *s)
{
	assert(s->op == SR_SERIALIZE);
	assert( sr_meta_serialize(c, s) == 0 );
	return 0;
}

static void
sr_meta_serialize0(void)
{
	uint32_t value0 = 8;
	uint64_t value1 = UINT64_MAX;

	srmeta ctls[100];
	srmeta *pc = &ctls[0];

	srmeta *v;
	v       = sr_m(NULL, &pc, sr_meta_serialize_f, "u32", SS_U32, &value0);
	v->next = sr_m(NULL, &pc, sr_meta_serialize_f, "u64", SS_U64, &value1);
	srmeta *ns;
	ns      = sr_M(NULL, &pc, NULL, "test", SS_UNDEF, v, SR_NS, NULL);
	srmeta *root;
	root    = sr_M(NULL, &pc, NULL, "test", SS_UNDEF, ns, SR_NS, NULL);

	ssbuf buf;
	ss_bufinit(&buf);
	srmetastmt stmt = {
		.op = SR_SERIALIZE,
		.path = NULL,
		.value = NULL,
		.valuetype = SS_UNDEF,
		.valuesize = 0,
		.serialize = &buf,
		.ptr = NULL,
		.r = &st_r.r
	};
	t( sr_metaexec(root, &stmt) == 0 );
	t( ss_bufused(&buf) != 0 );

	srmetadump *vp = (srmetadump*)buf.s;
	t( strcmp(sr_metakey(vp), "test.test.u32") == 0 );
	t( strcmp(sr_metavalue(vp), "8") == 0);
	vp = sr_metanext(vp);
	t( strcmp(sr_metakey(vp), "test.test.u64") == 0 );

	ss_buffree(&buf, &st_r.a);
}

stgroup *sr_meta_group(void)
{
	stgroup *group = st_group("srmeta");
	st_groupadd(group, st_test("set_ns", sr_meta_set_ns));
	st_groupadd(group, st_test("set_ns_trigger", sr_meta_set_ns_trigger));
	st_groupadd(group, st_test("set_u32", sr_meta_set_u32));
	st_groupadd(group, st_test("set_ns_u32", sr_meta_set_ns_u32));
	st_groupadd(group, st_test("set_ns_ns_u32_0", sr_meta_set_ns_ns_u32_0));
	st_groupadd(group, st_test("set_ns_ns_u32_1", sr_meta_set_ns_ns_u32_1));
	st_groupadd(group, st_test("set_ns_ns_u32_bad", sr_meta_set_ns_ns_u32_bad0));
	st_groupadd(group, st_test("serialize0", sr_meta_serialize0));
	return group;
}
