#ifndef ST_ASSERT_H_
#define ST_ASSERT_H_

/*
 * sophia database
 * sphia.org
 *
 * Copyright (c) Dmitry Simonenko
 * BSD License
*/

#define t(expr)                                                                \
	{                                                                          \
		if (!(expr)) {                                                         \
			fprintf(st_r.output,                                               \
			        ": failed (%s:%d) %s\n",                                   \
			        __FILE__,                                                  \
			        __LINE__,                                                  \
			        #expr);                                                    \
			fflush(st_r.output);                                               \
			abort();                                                           \
		}                                                                      \
		st_r.stat_stmt++;                                                      \
	}

#define t_api(expr)                                                            \
	{                                                                          \
		int size;                                                              \
		if ((expr) != 0) {                                                     \
			char *error = sp_getstring(env, "sophia.error", &size);            \
			fprintf(st_r.output,                                               \
			        ": Sophia API call \"" #expr                               \
			        "\" (%s:%d) failed with error: %s\n",                      \
			        __FILE__,                                                  \
			        __LINE__,                                                  \
			        error);                                                    \
			fflush(st_r.output);                                               \
			free(error);                                                       \
			abort();                                                           \
		}                                                                      \
		st_r.stat_stmt++;                                                      \
	}

#define t_string_eq(expr, value)                                               \
	{                                                                          \
		const char *evaluated_value = (expr);                                  \
		if (strcmp(evaluated_value, (value)) != 0) {                           \
			fprintf(st_r.output,                                               \
			        ": " #expr " failed (%s:%d) : expected=" #value            \
			        ", evaluated=\"%s\"\n",                                    \
			        __FILE__,                                                  \
			        __LINE__,                                                  \
			        evaluated_value);                                          \
			fflush(st_r.output);                                               \
			abort();                                                           \
		}                                                                      \
		st_r.stat_stmt++;                                                      \
	}

#endif
