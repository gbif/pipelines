package au.org.ala.sandbox;

import static au.org.ala.sampling.LayerCrawler.unzipFiles;
import static au.org.ala.sandbox.AvroStream.parseWildcardPath;

import au.org.ala.sampling.Field;
import au.org.ala.sampling.SamplingService;
import au.org.ala.utils.ALAFsUtils;
import java.io.*;
import java.net.URL;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.gbif.pipelines.core.pojo.HdfsConfigs;
import org.gbif.pipelines.core.utils.AvroFSInput;
import org.gbif.pipelines.core.utils.FsUtils;
import org.gbif.pipelines.io.avro.*;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.converter.jackson.JacksonConverterFactory;

/** Sampling for ALA sandbox. */
@Slf4j
public class Sampling {

  public static final String UNKNOWN_STATUS = "unknown";
  public static final String FINISHED_STATUS = "finished";
  public static final String ERROR_STATUS = "error";

  private static final int CHUNK_SIZE = 10000; // Number of records loaded in memory

  /**
   * Produces a CSV, outputPath + "/intersect-sorted.csv", with the following columns: id, lat, lng,
   * (and one column for each layer). This file is sorted by id and aligns with the input
   * index-record avro.
   *
   * @param hdfsConfigs HdfsConfigs
   * @param inputPath path to the sorted index-record.avro file
   * @param outputPath path to output directory where intersect-sorted.csv will be created
   * @param samplingServiceUrl URL of the sampling service
   * @throws IOException if an I/O error occurs
   */
  @SneakyThrows
  public static void sample(
      HdfsConfigs hdfsConfigs,
      String inputPath,
      String outputPath,
      String samplingServiceUrl,
      long samplingServicePollingMs,
      int samplingServiceBatchSize,
      int downloadRetries)
      throws IOException {
    FileSystem fs = FsUtils.getFileSystem(hdfsConfigs, inputPath);
    List<Path> paths = parseWildcardPath(fs, inputPath);

    List<File> sortedChunkFiles = new ArrayList<>();

    // Step 1: Read only latlng and id into sorted chunks, keeping a set of unique coordinates
    // Performance: improve by dumping coordinate/id pairs when producing IndexRecord instead of
    // extracting
    // from the IndexRecord avro
    TreeSet<String> uniqueCoordinates = new TreeSet<>();
    for (Path path : paths) {
      DatumReader<IndexRecord> reader = new SpecificDatumReader<>(IndexRecord.class);
      try (SeekableInput input =
              new AvroFSInput(fs.open(path), fs.getContentSummary(path).getLength());
          DataFileReader<IndexRecord> dataFileReader = new DataFileReader<>(input, reader)) {

        List<IndexRecord> chunk = new ArrayList<>(CHUNK_SIZE);
        while (dataFileReader.hasNext()) {
          IndexRecord record = dataFileReader.next();
          if (StringUtils.isNotEmpty(record.getLatLng())) {
            // extract only the id and latlng
            IndexRecord newRecord =
                IndexRecord.newBuilder()
                    .setId(record.getId())
                    .setLatLng(record.getLatLng())
                    .build();
            uniqueCoordinates.add(record.getLatLng());
            chunk.add(newRecord);
            if (chunk.size() == CHUNK_SIZE) {
              sortedChunkFiles.add(writeSortedChunkLatLng(chunk));
              chunk.clear();
            }
          }
        }
        if (!chunk.isEmpty()) {
          sortedChunkFiles.add(writeSortedChunkLatLng(chunk));
        }
      }
    }

    // Step 2: Merge sorted chunks (latlng, id)
    mergeSortedChunksLatLng(sortedChunkFiles, outputPath + "/latlngid-sorted.avro");

    // Step 3: Sample unique coordinates
    Retrofit retrofit =
        new Retrofit.Builder()
            .baseUrl(samplingServiceUrl)
            .addConverterFactory(JacksonConverterFactory.create())
            .validateEagerly(true)
            .build();

    SamplingService service = retrofit.create(SamplingService.class);

    String layerList =
        Objects.requireNonNull(service.getFields().execute().body()).stream()
            .filter(Field::getEnabled)
            .map(l -> String.valueOf(l.getId()))
            .collect(Collectors.joining(","));

    intersect(
        fs,
        layerList,
        uniqueCoordinates,
        outputPath,
        service,
        samplingServicePollingMs,
        samplingServiceBatchSize,
        downloadRetries);

    // delete tmp file
    new File(outputPath + "/latlngid-sorted.avro").delete();
  }

  @SneakyThrows
  private static File writeSortedChunkLatLng(List<IndexRecord> chunk) throws IOException {
    chunk.sort(Comparator.comparing(IndexRecord::getLatLng));
    File tempFile = File.createTempFile("sortedChunk", ".avro");
    tempFile.deleteOnExit();

    DatumWriter<IndexRecord> writer = new SpecificDatumWriter<>(IndexRecord.class);
    try (DataFileWriter<IndexRecord> dataFileWriter = new DataFileWriter<>(writer)) {
      dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.snappyCodec());
      dataFileWriter.create(IndexRecord.getClassSchema(), tempFile);
      for (IndexRecord record : chunk) {
        dataFileWriter.append(record);
      }
    }
    return tempFile;
  }

  @SneakyThrows
  private static void mergeSortedChunksLatLng(List<File> sortedChunkFiles, String outputPath) {
    PriorityQueue<AvroSort.AvroFileReader<IndexRecord>> pq =
        new PriorityQueue<>(
            Comparator.comparing((AvroSort.AvroFileReader<IndexRecord> r) -> r.peek().getLatLng()));

    for (File file : sortedChunkFiles) {
      AvroSort.AvroFileReader<IndexRecord> reader =
          new AvroSort.AvroFileReader<>(file, IndexRecord.class);
      if (reader.hasNext()) {
        pq.add(reader);
      }
    }

    DatumWriter<IndexRecord> writer = new SpecificDatumWriter<>(IndexRecord.class);
    try (DataFileWriter<IndexRecord> dataFileWriter = new DataFileWriter<>(writer)) {
      dataFileWriter.setCodec(org.apache.avro.file.CodecFactory.snappyCodec());
      dataFileWriter.create(IndexRecord.getClassSchema(), new File(outputPath));

      while (!pq.isEmpty()) {
        AvroSort.AvroFileReader<IndexRecord> reader = pq.poll();
        IndexRecord record = reader.next();
        dataFileWriter.append(record);
        if (reader.hasNext()) {
          pq.add(reader);
        } else {
          reader.close();
          pq.remove(reader);
        }
      }
    }
  }

  public static void intersect(
      FileSystem fs,
      String layers,
      TreeSet<String> coords,
      String outputDirectoryPath,
      SamplingService service,
      long pollingDelay,
      int batchSize,
      int downloadRetries)
      throws Exception {
    StringBuilder sb = new StringBuilder();
    int count = 0;
    Iterator<String> iterator = coords.iterator();

    File outputDir = new File(outputDirectoryPath + "/intersect");
    if (!outputDir.exists()) {
      outputDir.mkdirs();
    }

    while (iterator.hasNext()) {
      if (count != 0) {
        sb.append(",");
      }
      sb.append(iterator.next());
      count++;

      if (count == batchSize || !iterator.hasNext()) {
        String points = sb.toString();
        sb.setLength(0);
        count = 0;

        Instant batchStart = Instant.now();

        // Submit a job to generate a join
        Response<SamplingService.Batch> submit =
            service.submitIntersectBatch(layers, points).execute();
        String batchId = submit.body().getBatchId();

        String state = UNKNOWN_STATUS;
        while (!state.equalsIgnoreCase(FINISHED_STATUS) && !state.equalsIgnoreCase(ERROR_STATUS)) {
          Response<SamplingService.BatchStatus> status = service.getBatchStatus(batchId).execute();
          SamplingService.BatchStatus batchStatus = status.body();
          state = batchStatus.getStatus();

          Instant batchCurrentTime = Instant.now();

          log.debug(
              "batch ID {} - status: {} - time elapses {} seconds",
              batchId,
              state,
              Duration.between(batchStart, batchCurrentTime).getSeconds());

          if (!state.equals(FINISHED_STATUS)) {
            TimeUnit.MILLISECONDS.sleep(pollingDelay);
          } else {
            log.debug("Downloading sampling batch {}", batchId);

            if (downloadFile(fs, outputDirectoryPath, batchId, batchStatus, downloadRetries)) {
              String zipFilePath = outputDirectoryPath + "/" + batchId + ".zip";
              ReadableByteChannel readableByteChannel = ALAFsUtils.openByteChannel(fs, zipFilePath);
              InputStream zipInput = Channels.newInputStream(readableByteChannel);

              try (ZipInputStream zipInputStream = new ZipInputStream(zipInput)) {
                ZipEntry entry = zipInputStream.getNextEntry();
                while (entry != null) {
                  log.debug("Unzipping {}", entry.getName());

                  String unzippedOutputFilePath =
                      outputDirectoryPath + "/intersect/" + batchId + ".csv";
                  if (!entry.isDirectory()) {
                    unzipFiles(fs, zipInputStream, unzippedOutputFilePath);
                  }

                  zipInputStream.closeEntry();
                  entry = zipInputStream.getNextEntry();
                }
              }

              // delete zip file
              ALAFsUtils.deleteIfExist(fs, zipFilePath);

              log.debug("Sampling done");
            } else {
              log.error("Unable to download batch ID {}, download failed", batchId);
            }
          }
        }

        if (state.equals(ERROR_STATUS)) {
          log.error("Unable to download batch ID {}, error status", batchId);
        }
      }
    }

    // merge all csv files, prepending the id from the latlngid-sorted.avro file
    File[] files = outputDir.listFiles();
    if (files != null) {
      // filter for ".csv" files, sort the files by name, where the name is converted to an integer
      files =
          Arrays.stream(files)
              .filter(file -> file.getName().endsWith(".csv"))
              .sorted(
                  Comparator.comparingLong(
                      f -> Long.parseLong(f.getName().substring(0, f.getName().length() - 4))))
              .toArray(File[]::new);

      int skipped = 0;
      int recordCount = 0;

      String header = null;

      // merge intersection files with latlngid-sorted.avro
      try (AvroSort.AvroFileReader<IndexRecord> dataFileReader =
          new AvroSort.AvroFileReader<>(
              new File(outputDirectoryPath + "/latlngid-sorted.avro"), IndexRecord.class)) {
        try (BufferedWriter writer =
            new BufferedWriter(new FileWriter(outputDirectoryPath + "/intersect.csv"))) {
          for (File file : files) {
            try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
              String line;
              IndexRecord record = dataFileReader.next();
              int row = 0;
              while (record != null && (line = reader.readLine()) != null) {
                // records are expected to align with the sorted latlng values, but if not, try to
                // align anyway

                // handle the header
                if (line.startsWith("lat")) {
                  // only store header for the first file
                  if (row == 0) {
                    header = "id," + line;
                  }
                  continue;
                }
                row++;

                int commaIndex1 = line.indexOf(",");
                int commaIndex2 = line.indexOf(",", commaIndex1 + 1);
                String lat = line.substring(0, commaIndex1);
                String lng = line.substring(commaIndex1 + 1, commaIndex2);
                String latlng = lat + "," + lng;

                // iterate through the (potentially larger) latlngid-sorted.avro file until the
                // latlng matches
                while (record != null && !latlng.equals(record.getLatLng())) {
                  skipped++;
                  record = dataFileReader.next();
                }

                // should not happen
                if (record == null) {
                  break;
                }

                // write all records with this same latlng
                while (record != null && latlng.equals(record.getLatLng())) {
                  writer.write(record.getId() + "," + line);
                  writer.newLine();
                  record = dataFileReader.next();
                  recordCount++;
                }
              }

              writer.flush();
            }
          }
        }
      }

      // sort the output csv, intersect.csv (with java)
      CsvSort.sortCsvByFirstColumn(
          outputDirectoryPath + "/intersect.csv", outputDirectoryPath + "/intersect-sorted.csv");

      // delete tmp files
      for (File file : files) {
        file.delete();
      }
      new File(outputDirectoryPath + "/intersect.csv").delete();

      // write the header
      try (BufferedWriter writer =
          new BufferedWriter(new FileWriter(outputDirectoryPath + "/intersect-header.csv"))) {
        writer.write(header);
        writer.flush();
      }

      log.info(
          "Intersected {} records with {} unique coordinates and skipped {}",
          recordCount,
          coords.size(),
          skipped);
    }
  }

  // Download the batch file with a retries mechanism.
  private static boolean downloadFile(
      FileSystem fs,
      String outputDirectoryPath,
      String batchId,
      SamplingService.BatchStatus batchStatus,
      int downloadRetries) {

    for (int i = 0; i < downloadRetries; i++) {

      try {
        try (ReadableByteChannel inputChannel =
                Channels.newChannel(new URL(batchStatus.getDownloadUrl()).openStream());
            WritableByteChannel outputChannel =
                ALAFsUtils.createByteChannel(fs, outputDirectoryPath + "/" + batchId + ".zip")) {
          ByteBuffer buffer = ByteBuffer.allocate(512);
          while (inputChannel.read(buffer) != -1) {
            buffer.flip();
            outputChannel.write(buffer);
            buffer.clear();
          }
          return true;
        }

      } catch (IOException e) {
        log.warn("Download for batch {} failed, retrying attempt {} of 5", batchId, i);
      }
    }
    return false;
  }
}
