
**NAME**

sp\_error - check error status

**SYNOPSIS**

```C
#include <sophia.h>

int sp_error(void *env);
```

**DESCRIPTION**

sp\_error(**env**): check if there any error leads to the shutdown.

Additionally, if any sophia error description can be accessed
through **sophia.error** field.

```C
int error_size;
char *error = sp_getstring(env, "sophia.error", &error_size);
if (error) {
	printf("error: %s\n", error);
	free(error);
}
```

**RETURN VALUE**

Returns 1 or 0.

**SEE ALSO**

[Sophia API](../tutorial/api.md)
