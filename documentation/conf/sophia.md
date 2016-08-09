
Sophia Environment
------------------

| name | type | description  |
|---|---|---|
| sophia.version | string, ro | Get current Sophia version. |
| sophia.version\_storage | string, ro  | Get current Sophia storage version. |
| sophia.build | string, ro | Get git commit id of a current build. |
| sophia.status | string, ro | Get Sophia status (eg. online). |
| sophia.errors | int, ro | Get a number of errors. |
| sophia.error | string, ro | Get last error description. |
| sophia.path | string  | Set current Sophia environment directory. |
| sophia.on\_log | function  | Set log function. |
| sophia.on\_log\_arg | string  | Set log function argument. |
