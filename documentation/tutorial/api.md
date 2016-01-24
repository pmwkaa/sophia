
Sophia API
----------

Sophia defines a small set of basic methods which can be applied to
any database object.

All API declarations are stored in a separate include file: **sophia.h**

Configuration, Control, Transactions and other objects are accessible
using the same methods. Methods are called depending on used objects.
Methods semantic may slightly change depending on used object.

All functions return either 0 on success, or -1 on error. The only exception are
functions that return a pointer. In that case NULL might indicate an error.

[sp_error()](../api/sp_error.md) function can be used to check if any fatal errors occured leading
to complete database shutdown. All created objects must be freed by
[sp_destroy()](../api/sp_destroy.md) function.

All methods are thread-safe and atomic.

Please take a look at the [API](../api/sp_env.md) manual section for additional details.

* [sp_env()](../api/sp_env.md)
* [sp_document()](../api/sp_document.md)
* [sp_setstring()](../api/sp_setstring.md)
* [sp_setint()](../api/sp_setint.md)
* [sp_getobject()](../api/sp_getobject.md)
* [sp_getstring()](../api/sp_getstring.md)
* [sp_getint()](../api/sp_getint.md)
* [sp_open()](../api/sp_open.md)
* [sp_destroy()](../api/sp_destroy.md)
* [sp_error()](../api/sp_error.md)
* [sp_poll()](../api/sp_poll.md)
* [sp_drop()](../api/sp_drop.md)
* [sp_set()](../api/sp_set.md)
* [sp_upsert()](../api/sp_upsert.md)
* [sp_delete()](../api/sp_delete.md)
* [sp_get()](../api/sp_get.md)
* [sp_cursor()](../api/sp_cursor.md)
* [sp_begin()](../api/sp_begin.md)
* [sp_commit()](../api/sp_commit.md)
