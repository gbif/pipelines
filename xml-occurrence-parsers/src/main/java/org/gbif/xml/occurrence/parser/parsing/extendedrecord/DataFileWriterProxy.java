package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

/**
 * Proxy class for avro DataFileWriter, created to avoid an issue during file writing
 */
public class DataFileWriterProxy {

  private final DataFileWriter<ExtendedRecord> dataFileWriter;

  public DataFileWriterProxy(DataFileWriter<ExtendedRecord> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
  }

  /**
   * Synchronized append method, helps avoid the ArrayIndexOutOfBoundsException
   */
  public synchronized void append(ExtendedRecord extendedRecord) throws IOException {
    dataFileWriter.append(extendedRecord);
  }
}
