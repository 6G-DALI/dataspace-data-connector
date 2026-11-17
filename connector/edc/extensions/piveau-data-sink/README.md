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

## Files that are not uploaded

Two kinds of file in the source bucket never reach the data lake. The sink skips them before it looks at their
extension, writes no status object for them (see below), and logs the name it skipped.

| Skipped | How it is chosen |
|---|---|
| The names in `edc.files.ignore` | A setting in the connector's `.properties` file: a comma-separated list of file names, for example `edc.files.ignore=README.md, Thumbs.db, .DS_Store`. A name matches a file's own name (the last segment of its path) as a whole, ignoring case. Spaces around a name and empty entries are dropped. Empty or absent: nothing is ignored. The names in effect are logged at startup. |
| Every file whose name ends in `.status` | The status objects this sink writes (next section). |

`edc.files.ignore` applies to every file, `metadata.json` included, so listing `metadata.json` stops the dataset
from being registered. It matches names, not patterns: `*.tmp` is not a pattern.

## Status files in the source bucket

When a file is complete in the data lake, the sink writes an object named like the source file plus `.status` next
to it in the **source** bucket: `exp-1/data.csv` gets `exp-1/data.csv.status`. The object holds the UUID generated
for the file in the data lake, as plain text and nothing else.

- **Data files:** the file's own UUID, the one in `<dataset-uuid>/<file-uuid>.csv`.
- **`metadata.json`:** the dataset's directory UUID, since it keeps its name in the lake.
- **When:** after the upload has finished and before the RabbitMQ message. A file the data lake already holds with
  the same content is not uploaded a second time and gets its status object, so files transferred before this
  feature get one on the next transfer. An existing status object that already holds the UUID is not rewritten.
- **Absent status:** the file has not been uploaded, or not completely. The sink writes nothing when an upload
  fails.
- **Where:** the bucket named in the transfer's *source* data address (`endpoint`, `bucketName`, `accessKey`,
  `secretKey` of the `MinioAsset`), under the file's full key. The source key therefore needs write permission. A
  status object that cannot be written is logged as a warning and does not fail the transfer. If the source address
  has no S3 endpoint, bucket and keys, no status objects are written and the log says so.
- **Never uploaded:** a `.status` object listed in the source bucket on a later transfer is skipped like any other
  file ending in `.status`.

## Catalogue registration

This extension **no longer talks to Piveau**. Datasets and distributions are registered by the
**s3-asset-monitor** (`project-dali/code/s3-asset-monitor`), which watches the data lake and
handles each new `metadata.json` and data file. The `piveauUrl`, `piveauApiKey` and
`piveauKeycloak*` destination properties and the `edc.dali.connector.url` setting are no longer
read, so the transfer request no longer needs to carry them. See that project's README for
the Piveau authentication setup.

The `PiveauData` type name is kept so existing contracts and transfer scripts keep working.
