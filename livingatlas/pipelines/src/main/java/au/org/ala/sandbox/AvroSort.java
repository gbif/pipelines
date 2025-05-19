package au.org.ala.sandbox;

import static au.org.ala.sandbox.AvroStream.parseWildcardPath;

import java.io.*;
import java.util.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.AvroFSInput;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.Record;

/** A class to sort class Record Avro files by the id field, and a reader for the sorted file. */
@Slf4j
public class AvroSort {

  private static final int CHUNK_SIZE = 10000; // Number of records loaded in memory

  /**
   * Sort avro file/s by id, into a single file
   *
   * @param hdfsConfigs HdfsConfigs
   * @param clazz extended Record class e.g. IndexRecord.class
   * @param schema schema of the extended Record class e.g. IndexRecord.getClassSchema()
   * @param inputPath input path to .avro file or directory containing >= 1 .avro files
   * @param outputPath output path to the .avro that will be created
   * @throws IOException if an I/O error occurs
   */
  public static <T extends Record> void sortAvroFile(
      HdfsConfigs hdfsConfigs, Class<T> clazz, Schema schema, String inputPath, String outputPath)
      throws IOException {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, inputPath);
    List<Path> paths = parseWildcardPath(fs, inputPath);

    List<File> sortedChunkFiles = new ArrayList<>();

    // Step 1: Read and sort chunks
    for (Path path : paths) {
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      try (SeekableInput input =
              new AvroFSInput(fs.open(path), fs.getContentSummary(path).getLength());
          DataFileReader<T> dataFileReader = new DataFileReader<>(input, reader)) {

        List<T> chunk = new ArrayList<>(CHUNK_SIZE);
        while (dataFileReader.hasNext()) {
          chunk.add(dataFileReader.next());
          if (chunk.size() == CHUNK_SIZE) {
            sortedChunkFiles.add(writeSortedChunk(chunk, clazz, schema));
            chunk.clear();
          }
        }
        if (!chunk.isEmpty()) {
          sortedChunkFiles.add(writeSortedChunk(chunk, clazz, schema));
        }
      }
    }

    // Step 2: Merge sorted chunks
    mergeSortedChunks(sortedChunkFiles, clazz, schema, outputPath);
  }

  @SneakyThrows
  private static <T extends Record> File writeSortedChunk(
      List<T> chunk, Class<T> clazz, Schema schema) throws IOException {
    chunk.sort(Comparator.comparing(Record::getId));
    File tempFile = File.createTempFile("sortedChunk", ".avro");
    tempFile.deleteOnExit();

    DatumWriter<T> writer = new SpecificDatumWriter<>(clazz);
    try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer)) {
      dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.snappyCodec());
      dataFileWriter.create(schema, tempFile);
      for (T record : chunk) {
        dataFileWriter.append(record);
      }
    }
    return tempFile;
  }

  @SneakyThrows
  private static <T extends Record> void mergeSortedChunks(
      List<File> sortedChunkFiles, Class<T> clazz, Schema schema, String outputPath) {
    PriorityQueue<AvroFileReader<T>> pq =
        new PriorityQueue<>(Comparator.comparing((AvroFileReader<T> r) -> r.peek().getId()));

    for (File file : sortedChunkFiles) {
      AvroFileReader<T> reader = new AvroFileReader<>(file, clazz);
      if (reader.hasNext()) {
        pq.add(reader);
      }
    }

    DatumWriter<T> writer = new SpecificDatumWriter<>(clazz);
    try (DataFileWriter<T> dataFileWriter = new DataFileWriter<>(writer)) {
      dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.snappyCodec());
      dataFileWriter.create(schema, new File(outputPath));

      while (!pq.isEmpty()) {
        AvroFileReader<T> reader = pq.poll();
        T record = reader.next();
        dataFileWriter.append(record);
        if (reader.hasNext()) {
          pq.add(reader);
        } else {
          reader.close();
          //          pq.remove(reader);
        }
      }
    }
  }

  /**
   * Reader for avro files sorted by the id field. It is used for both merging sorted avro files and
   * joining sorted avro files.
   */
  public static class AvroFileReader<T extends Record> implements AutoCloseable {
    private final DataFileReader<T> dataFileReader;

    T nextRecord = null;

    // File must be sorted by the id field
    public AvroFileReader(File file, Class<T> clazz) throws IOException {
      DatumReader<T> reader = new SpecificDatumReader<>(clazz);
      this.dataFileReader = new DataFileReader<>(file, reader);

      if (dataFileReader.hasNext()) {
        nextRecord = dataFileReader.next();
      }
    }

    public boolean hasNext() {
      return nextRecord != null;
    }

    public T next() {
      if (nextRecord != null) {
        T record = nextRecord;
        if (dataFileReader.hasNext()) {
          nextRecord = dataFileReader.next();
        } else {
          nextRecord = null;
        }
        return record;
      } else {
        return null;
      }
    }

    public T peek() {
      return nextRecord;
    }

    @Override
    public void close() {
      try {
        dataFileReader.close();
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      }
    }

    /**
     * Get the next record if it has the given id, otherwise return the default value.
     *
     * <p>This is only suitable when the records are sorted by id.
     *
     * @param id record id that is required
     * @param defaultValue the default value returned when the next available record does not have
     *     the required id
     * @return record required or the defaultValue
     */
    public T getOrDefault(String id, T defaultValue) {
      if (nextRecord != null && nextRecord.getId().equals(id)) {
        return next();
      }
      return defaultValue;
    }
  }
}
