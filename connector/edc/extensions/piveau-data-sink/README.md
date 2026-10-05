# piveau-data-sink

EDC data sink for transfers of type `PiveauData`. It writes the transferred files to a
MinIO/S3 destination (the data lake), in a bucket named `edc.experiment.prefix`. Nothing in the
lake carries the testbed's own directory or file names:

- A dataset's directory is a **random UUID**, not its directory name at the testbed. The first
  file seen for a source directory creates the UUID, and the mapping is stored in the bucket as
  `.datasets/<edc.experiment.prefix>-<source dir>` so it survives restarts.
- `.json` (dataset `metadata.json`) is uploaded as `<dataset-uuid>/<file>`, keeping its name.
- `.csv` is uploaded as `<dataset-uuid>/<file-uuid>.csv` (original extension kept). The file's
  UUID is also remembered, as `.files/<edc.experiment.prefix>-<source dir>/<file name>`, so
  sending the same file again overwrites the same object instead of adding a copy. The
  original file name is stored as the object's `original-name` user metadata (URL-encoded).
  The RabbitMQ message still carries the original `<dataset dir>/<file>.csv` name, since that
  is what the testbed knows.
  Then a completion message is published to RabbitMQ if
  `edc.rabbitmq.host` and `edc.rabbitmq.queue` are set. If the S3 upload fails and the
  destination has a `baseUrl`, the CSV is POSTed there instead.

Destination data address properties: `endpoint`, `bucketName`, `accessKey`, `secretKey`
(required), `prefix`, and `baseUrl` / `authKey` for the HTTP fallback.

## Catalogue registration

This extension **no longer talks to Piveau**. Datasets and distributions are registered by the
**s3-asset-monitor** (`project-dali/code/s3-asset-monitor`), which watches the data lake and
handles each new `metadata.json` and data file. The `piveauUrl`, `piveauApiKey` and
`piveauKeycloak*` destination properties and the `edc.dali.connector.url` setting are no longer
read, so the transfer request no longer needs to carry them. See that project's README for
the Piveau authentication setup.

The `PiveauData` type name is kept so existing contracts and transfer scripts keep working.
