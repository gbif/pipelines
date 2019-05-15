package org.gbif.converters.parser.xml.parsing.extendedrecord;

import java.io.IOException;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.DataFileWriter;

import lombok.AllArgsConstructor;

/** Sync class for avro DataFileWriter, created to avoid an issue during file writing */
@AllArgsConstructor
public class SyncDataFileWriter {

  private final DataFileWriter<ExtendedRecord> dataFileWriter;

  /** Synchronized append method, helps avoid the ArrayIndexOutOfBoundsException */
  public synchronized void append(ExtendedRecord extendedRecord) throws IOException {
    dataFileWriter.append(extendedRecord);
  }
}
