
### V1
 - it's reliable and fast
 - supports the ["legacy" pypi API](https://warehouse.pypa.io/api-reference/legacy.html)
 - support the /package/json api
 - support the JSON API


### V2
 - it scales. all data is served from S3. no other dependencies
 - works with pip local caching (since signed urls are hidden from client)

## Todo
 - support the /package/json api
 - support the JSON API
 - confirm largest filesize supported for upload
 - multi-node cache invalidation
 - fully support legacy upload API (handle metadata)
 - load testing
 - streaming uploads
 - handle storage backend downtime
 - handle storage backend errors
 - localfile backend - reuse nginx cache?
 - restrict filetypes
 - create empty indexes at startup
 - create json endpoints
 - add ability to reuse pypicloud s3 bucket?
 - ensure proper headers are set for pip local caching
 - create proper work queue instead of just using background tasks
 - add tests
 - add CI
 - handle dual uploads of identically named files?
 - create hostname migration script (rebuild all index files)
 - parse metadata from wheels (not even pypi.org does this?)

### V3
 - act as a caching proxy to pypi.org
 - handle https
 - support the XMLRPC API