package org.gbif.converters.parser.xml.parsing.extendedrecord;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

/** Sync class for avro DataFileWriter, created to avoid an issue during file writing */
public class SyncDataFileWriter {

  private final DataFileWriter<ExtendedRecord> dataFileWriter;

  public SyncDataFileWriter(DataFileWriter<ExtendedRecord> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
  }

  /** Synchronized append method, helps avoid the ArrayIndexOutOfBoundsException */
  public synchronized void append(ExtendedRecord extendedRecord) throws IOException {
    dataFileWriter.append(extendedRecord);
  }
}
