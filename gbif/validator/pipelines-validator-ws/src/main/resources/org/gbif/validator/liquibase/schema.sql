--
--  validation
--

CREATE TYPE enum_validation_status AS ENUM ('DOWNLOADING','SUBMITTED', 'RUNNING', 'FINISHED', 'FAILED');
CREATE TYPE enum_validation_file_format AS ENUM ('DWCA', 'TABULAR', 'SPREADSHEET');
CREATE TABLE validation
(
  key uuid NOT NULL PRIMARY KEY,
  status enum_validation_status NOT NULL,
  file_format enum_validation_file_format,
  username varchar(255) NOT NULL,
  file varchar(255) NOT NULL,
  file_size bigint,
  created timestamp with time zone NOT NULL DEFAULT now(),
  result text,
  modified timestamp with time zone NOT NULL DEFAULT now(),
  deleted timestamp with time zone
);
