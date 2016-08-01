
**NAME**

sp\_delete - delete operation

**SYNOPSIS**

```C
#include <sophia.h>

int sp_delete(void *object, void *document);
```

**DESCRIPTION**

sp\_delete(**database**, document): do a single-statement transaction.

sp\_delete(**transaction**, document): do a key deletion as a part of multi-statement transaction.

As a part of a transactional statement a key-value document must be prepared using
[sp\_document()](../api/sp_document.md) method. First argument of [sp\_document()](../api/sp_document.md)
method must be a database object.

Object must be prepared by setting **key** fields. Value is not used for delete operation.
It is important that while setting **key** fields, only pointers are copied. Real
data copies only during first operation.

For additional information take a look at [sp\_document()](sp_document.md), [sp\_begin()](sp_begin.md)
and [Transactions](../crud/transactions.md).

**EXAMPLE**

```C
char key[] = "key";
void *o = sp_document(db);
sp_setstring(o, "key", key, sizeof(key));
sp_delete(db, o);
```

**RETURN VALUE**

On success, [sp\_delete()](sp_delete.md) returns 0. On error, it returns -1.

Database object commit: (1) rollback or (2) lock.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
