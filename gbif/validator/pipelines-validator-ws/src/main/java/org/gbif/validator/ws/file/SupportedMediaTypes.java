package org.gbif.validator.ws.file;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.springframework.http.MediaType;

/** Contains MediaType supported by the validator. */
public class SupportedMediaTypes {

  public static final String TEXT_CSV = "text/csv";
  public static final MediaType TEXT_CSV_TYPE = new MediaType("text", "csv");

  public static final String TEXT_TSV = "text/tab-separated-values";
  public static final MediaType TEXT_TSV_TYPE = new MediaType("text", "tab-separated-values");

  // .xls
  public static final String APPLICATION_EXCEL = "application/vnd.ms-excel";
  public static final MediaType APPLICATION_EXCEL_TYPE =
      new MediaType("application", "vnd.ms-excel");

  // .xlsx
  public static final String APPLICATION_OFFICE_SPREADSHEET =
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
  public static final MediaType APPLICATION_OFFICE_SPREADSHEET_TYPE =
      new MediaType("application", "vnd.openxmlformats-officedocument.spreadsheetml.sheet");

  // .ods
  public static final String APPLICATION_OPEN_DOC_SPREADSHEET =
      "application/vnd.oasis.opendocument.spreadsheet";
  public static final MediaType APPLICATION_OPEN_DOC_SPREADSHEET_TYPE =
      new MediaType("application", "vnd.oasis.opendocument.spreadsheet");

  // the common one is defined by com.sun.jersey.multipart.file.CommonMediaTypes.ZIP , this is
  // another used by some sites
  public static final String APPLICATION_XZIP_COMPRESSED = "application/x-zip-compressed";
  public static final MediaType APPLICATION_XZIP_COMPRESSED_TYPE =
      new MediaType("application", "x-zip-compressed");

  public static final String APPLICATION_GZIP = "application/gzip";

  private SupportedMediaTypes() {}

  public static final List<String> COMPRESS_CONTENT_TYPE =
      ImmutableList.of(org.apache.tika.mime.MediaType.APPLICATION_ZIP.toString(), APPLICATION_GZIP);

  public static final List<String> TABULAR_CONTENT_TYPES =
      ImmutableList.of(MediaType.TEXT_PLAIN_VALUE, TEXT_CSV, TEXT_TSV);

  public static final List<String> SPREADSHEET_CONTENT_TYPES =
      ImmutableList.of(
          APPLICATION_EXCEL, APPLICATION_OFFICE_SPREADSHEET, APPLICATION_OPEN_DOC_SPREADSHEET);
}
