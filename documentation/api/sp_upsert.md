
**NAME**

sp\_upsert - common get operation

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_upsert(void *object, void *document);

typedef int (*upsert_callback)(char **result,
                               char **key, int *key_size, int key_count,
                               char *src, int src_size,
                               char *upsert, int upsert_size,
                               void *arg);
```

**DESCRIPTION**

sp\_upsert(**database**, document): do a single-statement transaction.

sp\_upsert(**transaction**, document): do a key update as a part of multi-statement transaction.

As a part of a transactional statement a key-value document must be prepared using
[sp\_document()](../api/sp_document.md) method. First argument of [sp\_document()](../api/sp_document.md)
method must be an database object.

Object must be prepared by setting **key** and **value** fields.
It is important that while setting **key** and **value** fields, only pointers are copied. Real
data copies only during first operation.

Value field should contain user-supplied data, which should be enough to implement
custom update or insert logic.

To enable upsert command, a **db.database_name.index.upsert** and optionally
**db.database_name.index.upsert_arg** must be set to callback function pointer.

For additional information take a look at [sp\_document()](sp_document.md), [sp\_begin()](sp_begin.md)
and [Transactions](../crud/transactions.md) and [Upsert](../crud/upsert.md) sections.

**EXAMPLE**

See Sophia repository upsert [example](https://github.com/pmwkaa/sophia/blob/master/example/upsert.c).

**RETURN VALUE**

On success, [sp\_set()](sp_set.md) returns 0. On error, it returns -1.

Database object commit: (1) rollback or (2) lock.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
