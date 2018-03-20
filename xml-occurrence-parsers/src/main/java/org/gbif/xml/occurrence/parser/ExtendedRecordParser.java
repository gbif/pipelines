package org.gbif.xml.occurrence.parser;

import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ConverterTask;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.ParserFileUtils;
import org.gbif.xml.occurrence.parser.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.xml.occurrence.parser.parsing.validators.UniquenessValidator;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import java.util.zip.Deflater;

import com.google.common.base.Strings;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file
 */
public class ExtendedRecordParser {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordParser.class);

  private static final String FILE_PREFIX = ".response";

  private ExtendedRecordParser() {
    // NOP
  }

  /**
   * @param inputPath  path to directory with response files or a tar.xz archive
   * @param outputPath output path to avro file
   */
  public static void convertFromXML(String inputPath, String outputPath) {

    if (Strings.isNullOrEmpty(inputPath) || Strings.isNullOrEmpty(outputPath)) {
      throw new ParsingException("Input or output path must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);
    File outputFile = new File(outputPath);
    Schema schema = ExtendedRecord.getClassSchema();

    try (DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));
         Stream<Path> walk = Files.walk(inputFile.toPath());
         UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      // Create directories if they are absent
      if (Objects.nonNull(outputFile.getParentFile()) && !outputFile.getParentFile().exists()) {
        Files.createDirectories(outputFile.getParentFile().toPath());
      }

      dataFileWriter.setCodec(CodecFactory.deflateCodec(Deflater.BEST_SPEED));
      dataFileWriter.setFlushOnEveryBlock(false);
      dataFileWriter.create(schema, outputFile);

      // Class with sync method to avoid problem with writing
      SyncDataFileWriter writerWrapper = new SyncDataFileWriter(dataFileWriter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures = walk.filter(x -> x.toFile().isFile() && x.toString().endsWith(FILE_PREFIX))
        .map(Path::toFile)
        .map(file -> CompletableFuture.runAsync(new ConverterTask(file, writerWrapper, validator)))
        .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      try {
        Files.deleteIfExists(outputFile.toPath());
      } catch (IOException ioex) {
        LOG.error(ioex.getMessage(), ioex);
      }
      throw new ParsingException(ex);
    }
  }

}