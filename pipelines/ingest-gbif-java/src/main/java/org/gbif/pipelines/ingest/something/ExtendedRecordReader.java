package org.gbif.pipelines.ingest.something;

import java.io.File;
import java.util.HashMap;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

public class ExtendedRecordReader {

  public static HashMap<String, ExtendedRecord> readUniqueRecords(String verbatimPath) throws Exception {
    return readUniqueRecords(new File(verbatimPath));
  }

  public static HashMap<String, ExtendedRecord> readUniqueRecords(File verbatimPath) throws Exception {

    HashMap<String, ExtendedRecord> map = new HashMap<>();

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(verbatimPath, datumReader)) {
      while (dataFileReader.hasNext()) {
        ExtendedRecord next = dataFileReader.next();
        map.put(next.getId(), next);
      }
    }

    return map;
  }

}
