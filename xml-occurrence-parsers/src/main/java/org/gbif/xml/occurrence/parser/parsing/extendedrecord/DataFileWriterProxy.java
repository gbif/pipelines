package org.gbif.xml.occurrence.parser.parsing.extendedrecord;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;

import org.apache.avro.file.DataFileWriter;

public class DataFileWriterProxy {

  private final DataFileWriter<ExtendedRecord> dataFileWriter;

  public DataFileWriterProxy(DataFileWriter<ExtendedRecord> dataFileWriter) {
    this.dataFileWriter = dataFileWriter;
  }

  public synchronized void append(ExtendedRecord extendedRecord) throws IOException{
    dataFileWriter.append(extendedRecord);
  }
}
