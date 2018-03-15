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
import java.util.stream.Stream;

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

  private static final String UNCOMPRESS_CMD = "tar -xvzf ";
  private static final String FILE_PREFIX = ".response";
  private static final String TMP_PATH = "/tmp/";

  private ExtendedRecordParser() {
    // NOP
  }

  /**
   * @param inputPath path to directory with response files or a tar.xz archive
   * @param outputPath output path to avro file
   */
  public static void convertFromXML(String inputPath, String outputPath) {

    File inputFile = getInputFile(inputPath);
    File outputFile = new File(outputPath);

    Schema schema = ExtendedRecord.getClassSchema();

    try (DataFileWriter<ExtendedRecord> dataFileWriter = new DataFileWriter<>(new SpecificDatumWriter<>(schema));
         Stream<Path> walk = Files.walk(inputFile.toPath())) {

      // Create directories if they are absent
      if (Objects.nonNull(outputFile.getParentFile()) && !outputFile.getParentFile().exists()) {
        Files.createDirectories(outputFile.getParentFile().toPath());
      }

      dataFileWriter.setCodec(CodecFactory.deflateCodec(5));
      dataFileWriter.setFlushOnEveryBlock(false);
      dataFileWriter.create(schema, outputFile);

      // Proxy to avoid problem with writing
      DataFileWriterProxy writerWrapper = new DataFileWriterProxy(dataFileWriter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures = walk.filter(x -> x.toFile().isFile() && x.toString().endsWith(FILE_PREFIX))
        .map(Path::toFile)
        .map(file -> CompletableFuture.runAsync(new ConverterTask(file, writerWrapper)))
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
    } finally {
      // Close cache instance which was opened during processing ConverterTasks
      MapCache.getInstance().close();
    }
  }

  /**
   * @param inputPath path to a folder with xmls or a tar.xz archive
   * @return the new File object, path to xml files folder
   */
  private static File getInputFile(String inputPath) {
    // Check directory
    File inputFile = new File(inputPath);
    if (!inputFile.exists()) {
      LOG.error("Directory or file {} does not exist", inputFile.getAbsolutePath());
      throw new ParsingException("Directory or file " + inputFile.getAbsolutePath() + " does not exist");
    }

    // Uncompress if it is a tar.xz
    if (inputFile.isFile()) {
      return uncompress(inputFile);
    }

    return inputFile;
  }

  /**
   * Uncompress a tar.xz archive
   * @param inputFile - *.tar.xz file
   * @return the new File object, path to uncompressed files folder
   */
  private static File uncompress(File inputFile) {
    try {
      File parentFile = new File(inputFile.getParentFile().getAbsolutePath() + TMP_PATH);
      Files.createDirectories(parentFile.toPath());
      String cmd = UNCOMPRESS_CMD + inputFile.getAbsolutePath() + " -C " + parentFile.getAbsolutePath();
      Runtime.getRuntime().exec(cmd).waitFor();
      return parentFile;
    } catch (InterruptedException | IOException ex) {
      LOG.error("Directory or file {} does not exist", inputFile.getAbsolutePath());
      throw new ParsingException(ex);
    }
  }

}