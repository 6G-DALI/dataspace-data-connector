# piveau-data-sink

EDC data sink for transfers of type `PiveauData`. It writes the transferred files to a
MinIO/S3 destination (the data lake) under `<edc.experiment.prefix>-<dataset dir>/<file>` in a
bucket named `edc.experiment.prefix`:

- `.json` (dataset `metadata.json`) is uploaded as-is.
- `.csv` is uploaded, then a completion message is published to RabbitMQ if
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
