
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
#include <libso.h>
#include <libsv.h>
#include <libsl.h>
#include <libsd.h>
#include <libsi.h>
#include <libsx.h>
#include <libsy.h>
#include <libsc.h>
#include <libse.h>

sotype se_o[] =
{
	{ 0L,          "undefined"         },
	{ 0x9BA14568L, "destroyed"         },
	{ 0x06154834L, "env"               },
	{ 0x20490B34L, "env_conf"          },
	{ 0x6AB65429L, "env_conf_cursor"   },
	{ 0x00FCDE12L, "env_conf_kv"       },
	{ 0x64519F00L, "req"               },
	{ 0x2FABCDE2L, "document"          },
	{ 0x34591111L, "database"          },
	{ 0x63102654L, "database_cursor"   },
	{ 0x13491FABL, "transaction"       },
	{ 0x22FA0348L, "view"              },
	{ 0x45ABCDFAL, "cursor"            }
};
