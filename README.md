Script Executer

Objective of this project is to build an api to upload a shell scripts to AWS, execute script and return results using an API

Current API implementation

POST
/bucket
description: Upload shell scripts to s3 bucket
Form data:
    prefix: Username or any namespace
    local: file field(multipart form header format)

GET
/bucket
description: Get all prefixes/namespaces in scripts bucket
return format: json
return key: `data`(type: list)

GET
/bucket/:prefix
description: Get all script file names uploaded under prefix
return format: json
return key: `data` (type list)

GET
/bucket/:prefix/download
description: Download script file from bucket set by exact prefix


Remaining Items:

- Asyncronously execute scripts using Go Routines
- Validate that files uploaded are shell scripts only
- Error handling
- Dockerize service
- Deploy to Lambda
- Maybe a front end