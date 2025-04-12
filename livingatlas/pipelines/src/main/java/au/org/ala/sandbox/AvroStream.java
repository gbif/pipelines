package au.org.ala.sandbox;

import static org.gbif.pipelines.common.PipelinesVariables.Pipeline.AVRO_EXTENSION;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.AvroFSInput;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.Record;

/**
 * Provides a streaming version of the function
 * org.gbif.pipelines.core.io.AvroReader.readUniqueRecords
 */
@Slf4j
public class AvroStream {

  // This is a streaming version of org.gbif.pipelines.core.io.AvroReader.readUniqueRecords
  @SneakyThrows
  public static <T extends Record> Stream<T> streamUniqueRecords(
      HdfsConfigs hdfsConfigs, Class<T> clazz, String path, Runnable metrics) {

    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, path);
    List<Path> paths = parseWildcardPath(fs, path);

    // when there is only one path, it is immutable, make it mutable
    List<Path> pathsFinal = paths.size() == 1 ? new ArrayList<>(paths) : paths;

    // Read avro record from disk/hdfs
    DatumReader<T> reader = new SpecificDatumReader<>(clazz);

    Set<String> uniqueIds = new HashSet<>();

    // globals
    class Globals {
      public Path currentPath = null;
      DataFileReader<T> dataFileReader = null;
    }

    AtomicInteger counter = new AtomicInteger(0);

    Long startTime = System.currentTimeMillis();
    Globals globals = new Globals();
    Object safe = new Object();

    return Stream.generate(
            () -> {
              synchronized (safe) {
                try {
                  // does it still have records?
                  boolean hasNext = true;
                  while (hasNext) {
                    hasNext = false;

                    // get the next dataFileReader, if not set or at the end of the current
                    // dataFileReader
                    if (globals.dataFileReader == null || !globals.dataFileReader.hasNext()) {
                      // close the previous reader
                      if (globals.dataFileReader != null) {
                        globals.dataFileReader.close();
                      }

                      // get the next path
                      if (pathsFinal.isEmpty()) {
                        return null; // end of stream
                      } else {
                        globals.currentPath = pathsFinal.remove(0);
                      }

                      // open the next reader
                      SeekableInput input =
                          new AvroFSInput(
                              fs.open(globals.currentPath),
                              fs.getContentSummary(globals.currentPath).getLength());
                      globals.dataFileReader = new DataFileReader<>(input, reader);
                    }

                    // read the next record, looping again if it is a duplicate
                    T next = globals.dataFileReader.next();

                    if (uniqueIds.contains(next.getId())) {
                      hasNext = true;

                      log.warn("occurrenceId = {}, duplicates were found", next.getId());

                      // Increase metrics for duplicates
                      Optional.ofNullable(metrics).ifPresent(Runnable::run);
                    } else {
                      int count = counter.incrementAndGet();

                      if (count % 1000 == 0) {
                        long elapsed = System.currentTimeMillis() - startTime;
                        if (elapsed > 1000) {
                          try {
                            log.debug(
                                "Processed {} records in {} ms, {} records/s",
                                count,
                                elapsed,
                                count / (elapsed / 1000));
                          } catch (Exception e) {
                            log.error("Error calculating records per second", e);
                          }
                        }
                      }

                      uniqueIds.add(next.getId());
                      return next;
                    }
                  }
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
                return null;
              }
            })
        .takeWhile(Objects::nonNull);
  }

  /** Read multiple files, with the wildcard in the path */
  @SneakyThrows
  public static List<Path> parseWildcardPath(FileSystem fs, String path) {
    if (path.contains("*")) {
      Path pp = new Path(path).getParent();
      return FsUtils.getFilesByExt(fs, pp, AVRO_EXTENSION);
    }
    return Collections.singletonList(new Path(path));
  }
}
