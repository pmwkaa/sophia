
**NAME**

sp\_cursor - common cursor operation

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_cursor(void *env);
```

**DESCRIPTION**

sp\_cursor(**env**): create a cursor ready to be used with any database.

For additional information take a look at [Cursor](../crud/cursors.md) section.

**EXAMPLE**

```C
void *cursor = sp_cursor(env);
void *o = sp_document(db);
sp_setstring(o, "order", ">=", 0);
while ((o = sp_get(cursor, o))) {
	char *key = sp_getstring(o, "key", NULL);
	char *value = sp_getstring(o, "value", NULL);
	printf("%s = %s\n", key, value);
}
sp_destroy(cursor);
```

**RETURN VALUE**

On success, [sp\_cursor()](sp_cursor.md) returns cursor object handle.
On error, it returns NULL.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
