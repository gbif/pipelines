package org.gbif.pipelines.core.io;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import java.util.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.hadoop.fs.*;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.Record;

/** Avro format reader, reads {@link Record} based objects using sting or {@link List<Path>} path */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AvroReader {

  /**
   * Read {@link Record#getId()} unique records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read
   *     multiple files
   */
  public static <T extends Record> Map<String, T> readUniqueRecords(
      HdfsConfigs hdfsConfigs, Class<T> clazz, String path, Runnable metrics) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
    List<Path> paths = parseWildcardPath(fs, path);
    return readUniqueRecords(fs, clazz, paths, metrics);
  }

  /**
   * Read {@link Record#getId()} unique records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read
   *     multiple files
   */
  public static <T extends Record> Map<String, T> readUniqueRecords(
      HdfsConfigs hdfsConfigs, Class<T> clazz, String path) {
    return readUniqueRecords(hdfsConfigs, clazz, path, null);
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read
   *     multiple files
   */
  public static <T extends Record> Map<String, T> readRecords(
      HdfsConfigs hdfsConfigs, Class<T> clazz, String path) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
    List<Path> paths = parseWildcardPath(fs, path);
    return readRecords(fs, clazz, paths);
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read
   *     multiple files
   */
  public static <T extends SpecificRecordBase> List<T> readObjects(
      HdfsConfigs hdfsConfigs, Class<T> clazz, String path) {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
    List<Path> paths = parseWildcardPath(fs, path);
    return readObjects(fs, clazz, paths);
  }

  /**
   * Read {@link Record#getId()} unique records
   *
   * @param clazz instance of {@link Record}
   * @param paths list of paths to the files
   */
  @SneakyThrows
  private static <T extends Record> Map<String, T> readUniqueRecords(
      FileSystem fs, Class<T> clazz, List<Path> paths, Runnable metrics) {

    Map<String, T> map = new HashMap<>();
    Set<String> duplicateSet = new HashSet<>();

    for (Path path : paths) {
      // Read avro record from disk/hdfs
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      try (SeekableInput input =
              new AvroFSInput(fs.open(path), fs.getContentSummary(path).getLength());
          DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
        while (dataFileReader.hasNext()) {
          T next = dataFileReader.next();

          T saved = map.get(next.getId());
          if (saved == null && !duplicateSet.contains(next.getId())) {
            map.put(next.getId(), next);
          } else if (saved != null && !saved.equals(next)) {
            map.remove(next.getId());
            duplicateSet.add(next.getId());
            log.warn("occurrenceId = {}, duplicates were found", saved.getId());

            // Increase metrics for duplicates
            Optional.ofNullable(metrics).ifPresent(Runnable::run);
          }
        }
      }
    }

    return map;
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param paths list of paths to the files
   */
  @SneakyThrows
  private static <T extends SpecificRecordBase> List<T> readObjects(
      FileSystem fs, Class<T> clazz, List<Path> paths) {

    List<T> map = new ArrayList<>();

    for (Path path : paths) {
      // Deserialize ExtendedRecord from disk
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      try (SeekableInput input =
              new AvroFSInput(fs.open(path), fs.getContentSummary(path).getLength());
          DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
        while (dataFileReader.hasNext()) {
          T next = dataFileReader.next();
          map.add(next);
        }
      }
    }

    return map;
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param paths list of paths to the files
   */
  @SneakyThrows
  private static <T extends Record> Map<String, T> readRecords(
      FileSystem fs, Class<T> clazz, List<Path> paths) {

    Map<String, T> map = new HashMap<>();

    for (Path path : paths) {
      // Deserialize ExtendedRecord from disk
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      try (SeekableInput input =
              new AvroFSInput(fs.open(path), fs.getContentSummary(path).getLength());
          DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {
        while (dataFileReader.hasNext()) {
          T next = dataFileReader.next();
          map.put(next.getId(), next);
        }
      }
    }

    return map;
  }

  /** Read multiple files, with the wildcard in the path */
  @SneakyThrows
  private static List<Path> parseWildcardPath(FileSystem fs, String path) {
    if (path.contains("*")) {
      Path pp = new Path(path).getParent();
      return FsUtils.getFilesByExt(fs, pp, AVRO_EXTENSION);
    }
    return Collections.singletonList(new Path(path));
  }
}
