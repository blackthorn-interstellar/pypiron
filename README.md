# PypIron

A fast, reliable, and scalable pypi server.

## Features
 - No database required


## Getting Started
1. Create an S3 bucket and credentials with permissions to read/write to that bucket
2. Run the docker container
```bash
docker run --rm -it -p 80:80 \
  -e PYPIRON_PACKAGES_S3_URL=s3://<my_bucket_name> \
  -e PYPIRON_ADMIN_PASSWORD=< mypassword > \
  -e AWS_SECRET_ACCESS_KEY=<my_secret_key> \
  -e AWS_ACCESS_KEY_ID=<my_access_key> \ 
  pypiron:latest
```
3. use twine to upload files



## storage file structure

 - /index.json
 - /change-log/
 - /packages/
   - __index.html
   - __index.json
   - package-name/
     - __index.html
     - __index.json
     - files/
       - distribution(.whl|tar.gz)
       - distribution(.asc)
       - distribution.metadata.json



## Ecosystem
 - devpi-server
 - pypiserver
 - pypicloud
 - warehouse
 - gitlab

## useful docs
 - https://warehouse.pypa.io/api-reference/legacy.html
 - https://peps.python.org/pep-0426/
 - https://peps.python.org/pep-0503/
 - https://peps.python.org/pep-0691/
 - https://github.com/nchepanov/peps/blob/warehouse_json_api/pep-9999.rst
 - [making multi-service docker containers](https://docs.docker.com/config/containers/multi-service_container/)
 - [uwsgi-nginx docker container example](https://github.com/tiangolo/uwsgi-nginx-docker/blob/master/docker-images/python3.9.dockerfile)
