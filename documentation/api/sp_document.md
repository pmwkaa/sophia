
**NAME**

sp\_document - create a document object

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_document(void *object);
```

**DESCRIPTION**

sp\_document(**database**): create new document for a transaction on a selected database.

The [sp\_document()](sp_document.md) function returns an object which is intended
to be used in by any [CRUD](../crud/transactions.md) operations. Document might contain a
key-value pair with any additional metadata.

**EXAMPLE**

```C
void *o = sp_document(db);
sp_setstring(o, "key", "hello", 0);
sp_setstring(o, "value", "world", 0));
sp_set(db, o);
```

**RETURN VALUE**

On success, [sp\_document()](sp_document.md) returns an object pointer.
On error, it returns NULL.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
