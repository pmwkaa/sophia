
AMQ Filter
----------

AMQF stands for 'Approximate Member Query Filter'. The filter can be turned on to reduce
a number of possible disk accesses during point-looks using
[sp_get()](../api/sp_get.md) or [sp_delete()](../api/sp_delete.md). The filter is not used for range queries
by [sp_cursor()](../api/sp_cursor.md), cursor implementation has its own caching scheme.

Following variable can be set to enable or disable AMQF usage: **db.database_name.amqf**

```C
sp_setint(env, "db.test.amqf", 1);
```

By default the filter is turned off, because normally there is no need for it. But there
are some cases, when it can be useful.

Sophia uses the [Quotient Filter](https://en.wikipedia.org/wiki/Quotient_filter) for the AMQF purpose.
