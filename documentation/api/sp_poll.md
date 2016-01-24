
**NAME**

sp\_poll - get a completed asynchronous request

**SYNOPSIS**

```C
#include <sophia.h>

void *sp_poll(void *env);
```

**DESCRIPTION**

sp\_poll(**env**)

For additional information take a look at
[Asynchronous read](../crud/async_reads.md) section.

**RETURN VALUE**

On success, [sp\_poll()](sp_poll.md) returns a document handle.
If there are no completed requests, returns NULL.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
