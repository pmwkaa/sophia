
**NAME**

sp\_destroy - free or destroy an object

**SYNOPSIS**

```C
#include <sophia.h>

int sp_destroy(void *object);
```

**DESCRIPTION**

The [sp\_destroy()](sp_destroy.md) function is used to free memory allocated by
any Sophia object.

**EXAMPLE**

```C
void *o = sp_document(db);
void *result = sp_get(db, o);
if (result)
	sp_destroy(result);
```

**RETURN VALUE**

On success, [sp\_destroy()](sp_destroy.md) returns 0. On error, it returns -1.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
