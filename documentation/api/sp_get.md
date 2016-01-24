
**NAME**

sp\_get - common get operation

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_get(void *object, void *document);
```

**DESCRIPTION**

sp\_get(**database**, document): do a single-statement transaction read.

sp\_get(**transaction**, document): do a key search as a part of multi-statement transaction visibility.

[sp_get()](sp_get.md) method returns an document that is semantically equal to
[sp_document()](sp_document.md), but is read-only.

For additional information take a look at [sp\_begin()](sp_begin.md)
and [Transactions](../crud/transactions.md).

**EXAMPLE**

```C
void *o = sp_document(db);
sp_set(o, "key", "hello", 0);
void *result = sp_get(db, o);
if (result) {
	int valuesize;
	char *value = sp_getstring(result, "value", &valuesize);
	printf("%s\n", value);
	sp_destroy(result);
}
```

**RETURN VALUE**

On success, [sp\_get()](sp_get.md) returns a document handle.
If an object is not found, returns NULL.
On error, it returns NULL.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
