
Configuration
--------------

Every Sophia configuraton, monitoring, database creation, etc. is done
using sysctl-alike interface.

Operations [sp_setstring()](../api/sp_setstring.md), [sp_getstring()](../api/sp_getstring.md),
[sp_setint()](../api/sp_setint.md), [sp_getint()](../api/sp_getint.md), [sp_getobject()](../api/sp_getobject.md)
and [sp_cursor()](../api/sp_cursor.md) are used to set, get and iterate through configuration fields.

Most of the configuration can only be changed before opening an environment.

Any error description can be accessed through **sophia.error** field.

Set example:

```C
void *env = sp_env()
sp_setstring(env, "sophia.path", "./sophia", 0);
sp_open(env);
```

Get example:

```C
int error_size;
char *error = sp_getstring(env, "sophia.error", &error_size);
if (error) {
	printf("error: %s\n", error);
	free(error);
}
```

To get a list of all system objects and configuration values:

```C
void *cursor = sp_getobject(env, NULL);
void *ptr = NULL;
while ((ptr = sp_get(cursor, ptr))) {
	char *key = sp_getstring(ptr, "key", NULL);
	char *value = sp_getstring(ptr, "value", NULL);
	printf("%s", key);
	if (value)
		printf(" = %s\n", value);
	else
		printf(" = \n");
}
sp_destroy(cursor);
```
