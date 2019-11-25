package org.gbif.pipelines.ingest.java.readers;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.gbif.pipelines.io.avro.Record;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroRecordReader {

  public static <T extends Record> Map<String, T> readUniqueRecords(Class<T> clazz, String path) {
    return readUniqueRecords(clazz, new File(path));
  }

  @SneakyThrows
  public static <T extends Record> Map<String, T> readUniqueRecords(Class<T> clazz, File path) {

    Map<String, T> map = new HashMap<>();
    Set<String> duplicateSet = new HashSet<>();

    // Deserialize avro record from disk
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    try (DataFileReader<T> dataFileReader = new DataFileReader<>(path, reader)) {
      while (dataFileReader.hasNext()) {
        T next = dataFileReader.next();

        T saved = map.get(next.getId());
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

  public static <T extends Record> Map<String, T> readRecords(Class<T> clazz, String path) {
    return readRecords(clazz, new File(path));
  }

  @SneakyThrows
  public static <T extends Record> Map<String, T> readRecords(Class<T> clazz, File path) {

    Map<String, T> map = new HashMap<>();

    // Deserialize ExtendedRecord from disk
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);
    try (DataFileReader<T> dataFileReader = new DataFileReader<>(path, reader)) {
      while (dataFileReader.hasNext()) {
        T next = dataFileReader.next();
        map.put(next.getId(), next);
      }
    }

    return map;
  }

}
