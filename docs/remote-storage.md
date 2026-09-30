# Remote storage

Path tables and `SparkCatalog` warehouses accept storage URIs. Cobble's native
filesystem is tried first; inaccessible or unsupported schemes fall back to the
Hadoop filesystem supplied by Spark. Hadoop plugins and their dependencies must
be installed on both driver and executors; the connector does not bundle them.

For native S3, use these options on `.read/.write.option(...)`, or with the
`spark.sql.catalog.<name>.` prefix for a catalog:

```properties
path=s3://bucket/warehouse
storage.option.endpoint=https://s3.example.com
storage.option.region=us-east-1
storage.option.access_key_id=<access-key>
storage.option.secret_access_key=<secret-key>
storage.option.enable_virtual_host_style=false
```

`storage.option.<provider-key>` forwards native provider options unchanged.
Specify endpoint protocol and region explicitly. The supported native S3
credential names (`access_key_id`, `secret_access_key`, `session_token`) are
removed from persisted routes and rebound from runtime options.
Other provider options can be persisted in routes: sensitive keys outside the
supported S3 credential names are rejected. URI user information is not supported.
Native file logging is disabled for remote table roots.

Configure Hadoop through Spark's `spark.hadoop.<key>` settings or the active
Spark context's Hadoop configuration, including `fs.<scheme>.impl`, endpoints,
credentials and credential providers. Do not pass Hadoop credentials through
`storage.option.*`. The effective Hadoop configuration and current identity are
captured for the operation and serialized to executors; they are never embedded
in native catalog metadata or scan/write plans. Scoped contexts prevent one job
from replacing another job's configuration. Overlapping storage roots with
different Hadoop contexts in one JVM are rejected while native handles are open.
The connector owns separate Hadoop filesystem instances and does not close
Spark's cached filesystem instances.

Hadoop HDFS uses its native overwrite rename. Hadoop S3A uses an abortable upload
to replace the destination before deleting the source; read/write failure aborts
the incomplete upload. This requires Hadoop's abortable-stream capability (tested
with Hadoop 3.3.2). Other Hadoop plugins can read existing tables, but replacing
existing metadata files requires the connector's `AtomicReplaceFileSystem`
contract; otherwise the write fails before deleting the existing destination.

Use one active writer job across processes and engines, and perform catalog
changes while writers are stopped. Remote storage has no distributed write lock
or fencing; local storage retains OS locks. Writes require
`spark.speculation=false`. Writer task attempts and stage attempts after the
initial attempt fail before opening a bucket. Automatic writer retries are not
supported; restart the complete write only after the previous job's tasks stop.

Captured delegation tokens are a baseline, not a renewal service. On executors,
newer tokens for the same user supplied by Spark take precedence; credentials
from other users are excluded. Spark's deployment must provide token renewal
for jobs that outlive the captured credentials. Fixed scans remain pinned to
their planned snapshot, and automatic snapshot expiration is not supported.
