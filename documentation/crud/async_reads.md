
Asynchronous reads
------------------

Asynchronous operations are automatically scheduled when using sp\_async(database) object
instead of database one. On complete: **scheduler.on\_event** callback function is
envoked.
