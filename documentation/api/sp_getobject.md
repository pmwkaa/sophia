
**NAME**

sp\_setstring, sp\_getstring, sp\_setint, sp\_getint, sp\_getobject - set or get configuration options

**SYNOPSIS**

```C
#include <sophia.h>

int      sp_setstring(void *object, const char *path, const void *ptr, int size);
void    *sp_getstring(void *object, const char *path, int *size);
int      sp_setint(void *object, const char *path, int64_t value);
int64_t  sp_getint(void *object, const char *path);
void    *sp_getobject(void *object, const char *path);
```

**DESCRIPTION**

For additional information take a look at the [Configuration](../tutorial/configuration.md) section.

**EXAMPLE**

```C
void *env = sp_env()
sp_setstring(env, "sophia.path", "./sophia", 0);
sp_open(env);
```

```C
char key[] = "key";
void *o = sp_document(db);
sp_setstring(o, "key", key, sizeof(key));
sp_setstring(o, "value", "hello world", 0));
sp_set(db, o);
```

```C
int error_size;
char *error = sp_getstring(env, "sophia.error", &error_size);
if (error) {
	printf("error: %s\n", error);
	free(error);
}
```

```C
void *db = sp_getobject(env, "db.test");
sp_drop(db);
```

**RETURN VALUE**

On success, [sp\_setstring()](sp_setstring.md) returns 0. On error, it returns -1.

On success, [sp\_getstring()](sp_getstring.md) returns string pointer.
On error or if the variable is not set, it returns NULL.

All pointers returned by [sp\_getstring()](sp_getstring.md) must be freed using free(3)
function. Exception is [sp\_document()](sp_document.md) object and
configuration cursor document.

On success, [sp\_setint()](sp_setint.md) returns 0. On error, it returns -1. On success,
[sp\_getint()](sp_getint.md) returns a numeric value. On error, it returns -1.

On success, [sp\_getobject()](sp_getobject.md) returns an object pointer.
On error or if the variable is not set, it returns NULL.

The database object returned by [sp\_getobject()](sp_getobject.md) increments its reference counter,
[sp\_destroy()](sp_destroy.md) can be used to decrement it.
This should be considered for online database close/drop cases.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
