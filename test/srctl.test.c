
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
srctl_set_cc(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	src root = {
		"test", SR_CC, NULL, NULL, NULL
	};
	srcstmt stmt = {
		.op = SR_CSET,
		.path = "test",
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(&root, &stmt) == -1 );
}

static int srctl_set_cc_trigger_called = 0;

static int
srctl_set_cc_trigger_f(src *c, srcstmt *s, va_list args)
{
	assert(s->op == SR_CSET);
	(void)c;
	(void)args;
	srctl_set_cc_trigger_called = 1;
	return 0;
}

static void
srctl_set_cc_trigger(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	src root = {
		"test", SR_CC, srctl_set_cc_trigger_f, NULL, NULL
	};
	srcstmt stmt = {
		.op = SR_CSET,
		.path = "test",
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(&root, &stmt) == 0 );
	t( srctl_set_cc_trigger_called == 1 );
}

static int
srctl_set_u32_f(src *c, srcstmt *s, va_list args)
{
	assert(s->op == SR_CSET);
	char *v = va_arg(args, char*);
	sr_cset(c, s, v);
	return 0;
}

static void
srctl_set_u32(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	src root = {
		"test", SR_CU32, srctl_set_u32_f, &value, NULL
	};
	srcstmt stmt = {
		.op = SR_CSET,
		.path = "test",
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(&root, &stmt, "5") == 0 );
	t( value == 5 );
}

static void
srctl_set_cc_u32(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	src u32cc = {
		"u32", SR_CU32, srctl_set_u32_f, &value, NULL
	};
	src root = {
		"test", SR_CC, NULL, &u32cc, NULL
	};
	srcstmt stmt = {
		.op = SR_CSET,
		.path = "test.u32",
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(&root, &stmt, "5") == 0 );
	t( value == 5 );
}

static void
srctl_set_cc_cc_u32(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	src u32v = {
		"u32", SR_CU32, srctl_set_u32_f, &value , NULL
	};
	src u32cc = {
		"test", SR_CC, NULL, &u32v, NULL
	};
	src root = {
		"test", SR_CC, NULL, &u32cc, NULL
	};
	srcstmt stmt = {
		.op = SR_CSET,
		.path = "test.test.u32",
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(&root, &stmt, "5") == 0 );
	t( value == 5 );
}

static void
srctl_set_cc_cc_u32_bad(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value = 0;
	src u32v = {
		"u32", SR_CU32, srctl_set_u32_f, &value, NULL
	};
	src u32cc = {
		"test", SR_CC, NULL, &u32v, NULL
	};
	src root = {
		"test", SR_CC, NULL, &u32cc, NULL
	};
	srcstmt stmt = {
		.op = SR_CSET,
		.path = "test.test.u64",
		.serialize = NULL,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(&root, &stmt, "5") == -1 );
	t( value == 0 );
}

static int
srctl_serialize_f(src *c, srcstmt *s, va_list args)
{
	assert(s->op == SR_CSERIALIZE);
	assert( sr_cserialize(c, s) == 0 );
	return 0;
}

static void
srctl_serialize0(stc *cx ssunused)
{
	ssa a;
	ss_aopen(&a, &ss_stda);
	sr r;
	srerror error;
	sr_errorinit(&error);
	sr_init(&r, &error, &a, NULL, NULL, SF_KV, SF_SRAW, NULL, NULL, NULL, NULL, NULL);
	uint32_t value0 = 8;
	uint64_t value1 = UINT64_MAX;

	src ctls[100];
	src *pc = &ctls[0];

	src *v;
	v       = sr_c(&pc, srctl_serialize_f, "u32", SR_CU32, &value0);
	v->next = sr_c(&pc, srctl_serialize_f, "u64", SR_CU64, &value1);
	src *cc;
	cc      = sr_c(&pc, NULL, "test", SR_CC, v);
	src *root;
	root    = sr_c(&pc, NULL, "test", SR_CC, cc);

	ssbuf buf;
	ss_bufinit(&buf);
	srcstmt stmt = {
		.op = SR_CSERIALIZE,
		.path = NULL,
		.serialize = &buf,
		.result = NULL,
		.ptr = NULL,
		.r = &r
	};
	t( sr_cexec(root, &stmt) == 0 );
	t( ss_bufused(&buf) != 0 );

	srcv *vp = (srcv*)buf.s;
	t( strcmp(sr_cvname(vp), "test.test.u32") == 0 );
	t( *(uint32_t*)sr_cvvalue(vp) == 8 );
	vp = sr_cvnext(vp);
	t( strcmp(sr_cvname(vp), "test.test.u64") == 0 );
	t( *(uint64_t*)sr_cvvalue(vp) == UINT64_MAX );

	ss_buffree(&buf, &a);
}

stgroup *srctl_group(void)
{
	stgroup *group = st_group("srctl");
	st_groupadd(group, st_test("set_cc", srctl_set_cc));
	st_groupadd(group, st_test("set_cc_trigger", srctl_set_cc_trigger));
	st_groupadd(group, st_test("set_u32", srctl_set_u32));
	st_groupadd(group, st_test("set_cc_u32", srctl_set_cc_u32));
	st_groupadd(group, st_test("set_cc_cc_u32", srctl_set_cc_cc_u32));
	st_groupadd(group, st_test("set_cc_cc_u32_bad", srctl_set_cc_cc_u32_bad));
	st_groupadd(group, st_test("serialize0", srctl_serialize0));
	return group;
}
