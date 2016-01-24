
**NAME**

sp\_begin - start a multi-statement transaction

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_begin(void *env);
```

**DESCRIPTION**

sp\_begin(**env**): create a transaction

During transaction, all updates are not written to the database
files until a [sp\_commit()](sp_commit.md) is called. All updates that were made
during transaction are available through [sp\_get()](sp_get.md) or by
using cursor.

The [sp\_destroy()](sp_destroy.md) function is used to discard changes of a
multi-statement transaction. All modifications that were made
during the transaction are not written to the log file.

No nested transactions are supported.

For additional information take a look at [Transactions](../crud/transactions.md) and
[Deadlock](../crud/deadlocks.md) sections.

**EXAMPLE**

```C
void *a = sp_getobject(env, "db.database_a");
void *b = sp_getobject(env, "db.database_b");

char key[] = "hello";
char value[] = "world";

/* begin a transaction */
void *transaction = sp_begin(env);

void *o = sp_document(a);
sp_setstring(o, "key", key, sizeof(key));
sp_setstring(o, "value", value, sizeof(value));
sp_set(transaction, o);

o = sp_document(b);
sp_setstring(o, "key", key, sizeof(key));
sp_setstring(o, "value", value, sizeof(value));
sp_set(transaction, o);

/* complete */
sp_commit(transaction);
```

**RETURN VALUE**

On success, [sp\_begin()](sp_begin.md) returns transaction object handle.
On error, it returns NULL.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
