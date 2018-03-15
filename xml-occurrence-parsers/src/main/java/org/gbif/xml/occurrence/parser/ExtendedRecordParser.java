package org.gbif.xml.occurrence.parser;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ConverterTask;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.DataFileWriterProxy;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.MapCache;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExtendedRecordParser {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordParser.class);

  private ExtendedRecordParser() {
    // NOP
  }

  public static void convertFromXML(String inputPath, String outputPath) {

    //
    File inputFile = new File(inputPath);
    if (!inputFile.exists()) {
      LOG.error("Directory or file {} does not exist", inputFile.getAbsolutePath());
      return;
    }

    //
    File outputFile = new File(outputPath);
    DatumWriter<ExtendedRecord> datumWriter = new SpecificDatumWriter<>(ExtendedRecord.SCHEMA$);

    try (DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(datumWriter);
         Stream<Path> walk = Files.walk(inputFile.toPath())) {

      if (Objects.nonNull(outputFile.getParentFile()) && !outputFile.getParentFile().exists()) {
        Files.createDirectories(outputFile.getParentFile().toPath());
      }

      dataFileWriter.create(ExtendedRecord.SCHEMA$, outputFile);
      dataFileWriter.setFlushOnEveryBlock(false);

      DataFileWriterProxy writerWrapper = new DataFileWriterProxy(dataFileWriter);

      // Run async
      CompletableFuture[] futures = walk.filter(Files::isRegularFile)
        .map(Path::toFile)
        .map(file -> CompletableFuture.runAsync(new ConverterTask(file, writerWrapper)))
        .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

    } catch (IOException | InterruptedException | ExecutionException | ParsingException ex) {
      LOG.error(ex.getMessage(), ex);
      try {
        Files.deleteIfExists(outputFile.toPath());
      } catch (IOException ioex) {
        LOG.error(ioex.getMessage(), ioex);
      }
    } finally {
      MapCache.getInstance().close();
    }
  }

}