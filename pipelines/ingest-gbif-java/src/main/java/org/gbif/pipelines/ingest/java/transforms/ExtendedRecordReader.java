package org.gbif.pipelines.ingest.java.transforms;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ExtendedRecordReader {

  public static Map<String, ExtendedRecord> readUniqueRecords(String verbatimPath) {
    return readUniqueRecords(new File(verbatimPath));
  }

  @SneakyThrows
  public static Map<String, ExtendedRecord> readUniqueRecords(File verbatimPath) {

    Map<String, ExtendedRecord> map = new HashMap<>();
    Set<String> duplicateSet = new HashSet<>();

    // Deserialize ExtendedRecord from disk
    DatumReader<ExtendedRecord> datumReader = new SpecificDatumReader<>(ExtendedRecord.class);
    try (DataFileReader<ExtendedRecord> dataFileReader = new DataFileReader<>(verbatimPath, datumReader)) {
      while (dataFileReader.hasNext()) {
        ExtendedRecord next = dataFileReader.next();

        ExtendedRecord saved = map.get(next.getId());
        if (saved == null && !duplicateSet.contains(next.getId())) {
          map.put(next.getId(), next);
        } else if (saved != null && !saved.equals(next)) {
          map.remove(next.getId());
          duplicateSet.add(next.getId());
          log.warn("occurrenceId = {}, duplicates were found", saved.getId());
        }

      }
    }

    return map;
  }

}
