package org.gbif.converters;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Stream;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.converter.ConverterToVerbatim;
import org.gbif.converters.converter.Metric;
import org.gbif.pipelines.core.converters.ExtendedRecordConverter;
import org.gbif.pipelines.core.io.DwcaExtendedRecordReader;
import org.gbif.pipelines.core.io.SyncDataFileWriter;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.utils.file.spreadsheet.CsvSpreadsheetConsumer;
import org.gbif.utils.file.spreadsheet.ExcelXmlConverter;

/** Converts DWC archive into {@link ExtendedRecord} AVRO file */
@Slf4j
@NoArgsConstructor(staticName = "create")
@Builder
public class DwcaToAvroConverter extends ConverterToVerbatim {

  public static void main(String... args) {
    if (args.length < 2) {
      throw new IllegalArgumentException("You must specify input and output paths");
    }
    String inputPath = args[0];
    String outputPath = args[1];
    boolean isFileCreated =
        DwcaToAvroConverter.create()
            .inputPath(inputPath)
            .outputPath(outputPath)
            .skipDeletion(true)
            .convert();
    log.info("Verbatim avro file has been created - {}", isFileCreated);
  }

  /**
   * Converts DWC archive into {@link ExtendedRecord} AVRO file
   *
   * @param inputPath Path to DWCA file
   * @param dataFileWriter AVRO data writer for {@link ExtendedRecord}
   */
  @Override
  protected Metric convert(Path inputPath, SyncDataFileWriter<ExtendedRecord> dataFileWriter)
      throws IOException {

    String realPath =
        Optional.of(inputPath)
            .filter(Files::isDirectory)
            .flatMap(this::normalizeSpreadsheetPath)
            .map(this::convertFromSpreadsheet)
            .orElse(inputPath)
            .toString();

    DwcaExtendedRecordReader reader;
    if (inputPath.toString().endsWith(".zip") || inputPath.toString().endsWith(".dwca")) {
      String tmp;
      if (Files.isDirectory(inputPath)) {
        tmp = inputPath.resolve("tmp").toString();
      } else {
        tmp = inputPath.getParent().resolve("tmp").toString();
      }
      reader = DwcaExtendedRecordReader.fromCompressed(realPath, tmp);
    } else {
      reader = DwcaExtendedRecordReader.fromLocation(realPath);
    }

    log.info("Exporting the DwC Archive to Avro started {}", realPath);

    // Read all records
    while (reader.advance()) {
      ExtendedRecord er = reader.getCurrent();
      if (!er.getId().equals(ExtendedRecordConverter.getRecordIdError())) {
        dataFileWriter.append(er);
      }
    }
    reader.close();

    return Metric.create(reader.getRecordsReturned(), reader.getOccurrenceRecordsReturned());
  }

  @SneakyThrows
  private Optional<Path> normalizeSpreadsheetPath(java.nio.file.Path path) {
    try (Stream<Path> list = Files.list(path)) {
      if (list.filter(x -> !x.toString().contains(".avro")).count() > 1) {
        return Optional.empty();
      }
    }
    try (Stream<java.nio.file.Path> list = Files.list(path)) {
      return list.filter(x -> x.toString().endsWith(".xlsx") || x.toString().endsWith(".ods"))
          .findFirst();
    }
  }

  @SneakyThrows
  private java.nio.file.Path convertFromSpreadsheet(java.nio.file.Path path) {
    java.nio.file.Path converted = path.resolveSibling("converted.csv");
    Predicate<String> extFn = ext -> path.toString().endsWith(ext);
    if (extFn.test(".xlsx") || extFn.test(".xls")) {
      ExcelXmlConverter.convert(
          path, new CsvSpreadsheetConsumer(new FileWriter(converted.toFile())));
    }
    return converted;
  }
}
