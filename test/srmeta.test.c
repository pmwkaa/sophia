
/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#include <libss.h>
#include <libsf.h>
#include <libsr.h>
#include <libst.h>

static void
srmeta_set_ns(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);

	srmeta root = {
		"test", 1, 0, SS_UNDEF, NULL, NULL, NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test",
		.value = NULL,
		.valuetype = SS_UNDEF,
		.valuesize = 0,
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == -1 );
}

static int srmeta_set_ns_trigger_called = 0;

static int
srmeta_set_ns_trigger_f(srmeta *m, srmetastmt *s)
{
	assert(s->op == SR_SET);
	(void)m;
	srmeta_set_ns_trigger_called = 1;
	return 0;
}

static void
srmeta_set_ns_trigger(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	srmeta root = {
		"test", 1, 0, SS_UNDEF, srmeta_set_ns_trigger_f, NULL, NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test",
		.value = NULL,
		.valuetype = SS_UNDEF,
		.valuesize = 0,
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( srmeta_set_ns_trigger_called == 1 );
}

static int
srmeta_set_u32_f(srmeta *c, srmetastmt *s)
{
	assert(s->op == SR_SET);
	sr_meta_eq(c, s);
	return 0;
}

static void
srmeta_set_u32(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	uint32_t set = 10;
	srmeta root = {
		.name = "test",
		.ns = 0,
		.rdonly = 0,
		.type = SS_U32,
		.function = srmeta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 10 );
}

static void
srmeta_set_ns_u32(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	uint32_t set = 15;
	srmeta u32 = {
		.name = "u32",
		.ns = 0,
		.rdonly = 0,
		.type = SS_U32,
		.function = srmeta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = srmeta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test.u32",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 15 );
}

static void
srmeta_set_ns_ns_u320(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	uint32_t set = 18;
	srmeta u32v = {
		.name = "u32",
		.ns = 0,
		.rdonly = 0,
		.type = SS_U32,
		.function = srmeta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta u32 = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = NULL,
		.value = &u32v,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = srmeta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test.test.u32",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 18 );
}

static void
srmeta_set_ns_ns_u321(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	uint64_t set = 18;
	srmeta u32v = {
		.name = "u32",
		.ns = 0,
		.rdonly = 0,
		.type = SS_U32,
		.function = srmeta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta u32 = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = NULL,
		.value = &u32v,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = srmeta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test.test.u32",
		.value = &set,
		.valuetype = SS_U64,
		.valuesize = sizeof(uint64_t),
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == 0 );
	t( value == 18 );
}

static void
srmeta_set_ns_ns_u32_bad0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	uint32_t set = 18;
	srmeta u32v = {
		.name = "u32",
		.ns = 0,
		.rdonly = 0,
		.type = SS_U32,
		.function = srmeta_set_u32_f,
		.value = &value,
		.ptr = NULL,
		.next = NULL
	};
	srmeta u32 = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = NULL,
		.value = &u32v,
		.ptr = NULL,
		.next = NULL
	};
	srmeta root = {
		.name = "test",
		.ns = 1,
		.rdonly = 0,
		.type = SS_UNDEF,
		.function = srmeta_set_u32_f,
		.value = &u32,
		.ptr = NULL,
		.next = NULL
	};
	srmetastmt stmt = {
		.op = SR_SET,
		.path = "test.test.u64",
		.value = &set,
		.valuetype = SS_U32,
		.valuesize = sizeof(uint32_t),
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(&root, &stmt) == -1 );
	t( value == 0 );
}

static int
srmeta_serialize_f(srmeta *c, srmetastmt *s)
{
	assert(s->op == SR_SERIALIZE);
	assert( sr_meta_serialize(c, s) == 0 );
	return 0;
}

static void
srmeta_serialize0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value0 = 8;
	uint64_t value1 = UINT64_MAX;

	srmeta ctls[100];
	srmeta *pc = &ctls[0];

	srmeta *v;
	v       = sr_meta(&pc, srmeta_serialize_f, "u32", SS_U32, &value0);
	v->next = sr_meta(&pc, srmeta_serialize_f, "u64", SS_U64, &value1);
	srmeta *ns;
	ns      = sr_meta(&pc, NULL, "test", SS_UNDEF, v);
	sr_metans(ns);
	srmeta *root;
	root    = sr_meta(&pc, NULL, "test", SS_UNDEF, ns);
	sr_metans(root);

	ssbuf buf;
	ss_bufinit(&buf);
	srmetastmt stmt = {
		.op = SR_SERIALIZE,
		.path = NULL,
		.value = NULL,
		.valuetype = SS_UNDEF,
		.valuesize = 0,
		.serialize = &buf,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_metaexec(root, &stmt) == 0 );
	t( ss_bufused(&buf) != 0 );

	srmetadump *vp = (srmetadump*)buf.s;
	t( strcmp(sr_metaname(vp), "test.test.u32") == 0 );
	t( *(uint32_t*)sr_metavalue(vp) == 8 );
	vp = sr_metanext(vp);
	t( strcmp(sr_metaname(vp), "test.test.u64") == 0 );
	t( *(uint64_t*)sr_metavalue(vp) == UINT64_MAX );

	ss_buffree(&buf, &a);
}

stgroup *srmeta_group(void)
{
	stgroup *group = st_group("srmeta");
	st_groupadd(group, st_test("set_ns", srmeta_set_ns));
	st_groupadd(group, st_test("set_ns_trigger", srmeta_set_ns_trigger));
	st_groupadd(group, st_test("set_u32", srmeta_set_u32));
	st_groupadd(group, st_test("set_ns_u32", srmeta_set_ns_u32));
	st_groupadd(group, st_test("set_ns_ns_u320", srmeta_set_ns_ns_u320));
	st_groupadd(group, st_test("set_ns_ns_u321", srmeta_set_ns_ns_u321));
	st_groupadd(group, st_test("set_ns_ns_u32_bad", srmeta_set_ns_ns_u32_bad0));
	st_groupadd(group, st_test("serialize0", srmeta_serialize0));
	return group;
}
