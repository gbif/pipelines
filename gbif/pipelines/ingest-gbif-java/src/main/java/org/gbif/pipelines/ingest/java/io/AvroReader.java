package org.gbif.pipelines.ingest.java.io;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.AvroFSInput;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.gbif.pipelines.ingest.utils.FsUtils;
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
      String hdfsSiteConfig, String coreSiteConfig, Class<T> clazz, String path) {
    FileSystem fs = FsUtils.getFileSystem(hdfsSiteConfig, coreSiteConfig, path);
    List<Path> paths = parseWildcardPath(fs, path);
    return readUniqueRecords(fs, clazz, paths);
  }

  /**
   * Read {@link Record#getId()} distinct records
   *
   * @param clazz instance of {@link Record}
   * @param path sting path, a wildcard can be used in the file name, like /a/b/c*.avro to read
   *     multiple files
   */
  public static <T extends Record> Map<String, T> readRecords(
      String hdfsSiteConfig, String coreSiteConfig, Class<T> clazz, String path) {
    FileSystem fs = FsUtils.getFileSystem(hdfsSiteConfig, coreSiteConfig, path);
    List<Path> paths = parseWildcardPath(fs, path);
    return readRecords(fs, clazz, paths);
  }

  /**
   * Read {@link Record#getId()} unique records
   *
   * @param clazz instance of {@link Record}
   * @param paths list of paths to the files
   */
  @SneakyThrows
  private static <T extends Record> Map<String, T> readUniqueRecords(
      FileSystem fs, Class<T> clazz, List<Path> paths) {

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
      RemoteIterator<LocatedFileStatus> files = fs.listFiles(pp, false);
      List<Path> paths = new ArrayList<>();
      while (files.hasNext()) {
        LocatedFileStatus next = files.next();
        Path np = next.getPath();
        if (next.isFile() && np.getName().endsWith(AVRO_EXTENSION)) {
          paths.add(np);
        }
      }
      return paths;
    }
    return Collections.singletonList(new Path(path));
  }
}
