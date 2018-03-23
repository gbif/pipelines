package org.gbif.pipelines.core.utils;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

public class AvroUtil {

  /**
   * Configure and create FileWriter with ExtendedRecord Format
   *
   * @return DataFileWriter with extendedRecord and appropriate configurations
   */
  public static DataFileWriter<ExtendedRecord> getExtendedRecordWriter(OutputStream os, int syncInterval,
                                                                       CodecFactory codec) throws IOException {
    return new DataFileWriter<>(new SpecificDatumWriter<ExtendedRecord>()).setSyncInterval(syncInterval)
      .setCodec(codec)
      .create(ExtendedRecord.getClassSchema(), os);
  }

}
