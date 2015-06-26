
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
#include <libse.h>

sotype se_o[] =
{
	{ 0L,          "undefined"       },
	{ 0x06154834L, "env"             },
	{ 0x38970021L, "env_async"       },
	{ 0x20490B34L, "env_meta"        },
	{ 0x6AB65429L, "env_meta_cursor" },
	{ 0x48991422L, "request"         },
	{ 0x2FABCDE2L, "object"          },
	{ 0x34591111L, "database"        },
	{ 0x24242489L, "database_async"  },
	{ 0x13491FABL, "transaction"     },
	{ 0x45ABCDFAL, "cursor"          },
	{ 0x71230BAFL, "snapshot"        }
};
