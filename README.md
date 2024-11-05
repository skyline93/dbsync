# dbsync

```ini
[database]
host=127.0.0.1
port=5432
user=test
password=test
database=test

[backup]
tables = table1, table2
output = backup.sql

[restore]
tables = table1, table2
output = restore.sql

[s3]
bucket = my-s3-bucket
aws_access_key = YOUR_AWS_ACCESS_KEY
aws_secret_key = YOUR_AWS_SECRET_KEY
region = YOUR_REGION
```
