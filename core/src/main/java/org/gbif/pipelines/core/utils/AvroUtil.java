package org.gbif.pipelines.core.utils;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;

/**
 * Utility class for writing Avro files on any file system in provided given schema
 */
public class AvroUtil {

  private AvroUtil(){
    
  }

  /**
   * Configure and create FileWriter with {@link ExtendedRecord} Format
   * @param os output stream to be used to create file writer
   * @param syncInterval the approximate number of uncompressed bytes to write in each block
   * @param codec compression codec to be used.
   * @return DataFileWriter object created with ExtendedRecord schema and provided configurations
   * @throws IOException
   */
  public static DataFileWriter<ExtendedRecord> getExtendedRecordWriter(OutputStream os, int syncInterval,
                                                                       CodecFactory codec) throws IOException {
    return new DataFileWriter<>(new SpecificDatumWriter<ExtendedRecord>()).setSyncInterval(syncInterval)
      .setCodec(codec)
      .create(ExtendedRecord.getClassSchema(), os);
  }

}
