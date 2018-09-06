package org.gbif.converters.parser.xml;

import org.gbif.converters.parser.xml.parsing.extendedrecord.ConverterTask;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ExecutorPool;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.converters.parser.xml.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Stream;

import com.google.common.base.Strings;
import org.apache.avro.file.DataFileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file */
public class ExtendedRecordConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordConverter.class);

  private static final String FILE_PREFIX = ".response";

  private final Executor executor;

  private ExtendedRecordConverter(int parallelism) {
    this.executor = ExecutorPool.getInstance(parallelism);
  }

  public static ExtendedRecordConverter crete(int parallelism) {
    return new ExtendedRecordConverter(parallelism);
  }

  /** @param inputPath path to directory with response files or a tar.xz archive */
  public void toAvro(String inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) {
    if (Strings.isNullOrEmpty(inputPath)) {
      throw new ParsingException("Input or output stream must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);

    try (Stream<Path> walk = Files.walk(inputFile.toPath());
        UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      // Class with sync method to avoid problem with writing
      SyncDataFileWriter syncWriter = new SyncDataFileWriter(dataFileWriter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures =
          walk.filter(x -> x.toFile().isFile() && x.toString().endsWith(FILE_PREFIX))
              .map(Path::toFile)
              .map(
                  file ->
                      CompletableFuture.runAsync(
                          new ConverterTask(file, syncWriter, validator), executor))
              .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

    } catch (Exception ex) {
      LOG.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }
}
