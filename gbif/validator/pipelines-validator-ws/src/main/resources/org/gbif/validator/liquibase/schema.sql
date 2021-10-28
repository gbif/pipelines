--
--  validation
--
CREATE TYPE enum_validation_status AS ENUM ('DOWNLOADING','SUBMITTED', 'RUNNING', 'FINISHED', 'ABORTED', 'FAILED', 'QUEUED');
CREATE TYPE enum_validation_file_format AS ENUM ('DWCA', 'TABULAR', 'SPREADSHEET', 'XML');
CREATE TABLE validation
(
  key uuid NOT NULL PRIMARY KEY,
  source_id text,
  installation_key uuid,
  notification_emails text[],
  status enum_validation_status NOT NULL,
  file_format enum_validation_file_format,
  username varchar(255) NOT NULL,
  file varchar(255) NOT NULL,
  file_size bigint,
  created timestamp with time zone NOT NULL DEFAULT now(),
  modified timestamp with time zone NOT NULL DEFAULT now(),
  deleted timestamp with time zone,
  dataset json,
  metrics json
);
