package org.gbif.converters.parser.xml;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.gbif.converters.parser.xml.parsing.extendedrecord.ConverterTask;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ExecutorPool;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.converters.parser.xml.parsing.extendedrecord.SyncDataFileWriter;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.apache.avro.file.DataFileWriter;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

/** Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file */
@Slf4j
public class ExtendedRecordConverter {

  private static final String FILE_PREFIX_RESPONSE = ".response";
  private static final String FILE_PREFIX_XML = ".xml";

  private final Executor executor;

  private ExtendedRecordConverter(int parallelism) {
    this.executor = ExecutorPool.getInstance(parallelism);
  }

  public static ExtendedRecordConverter crete(int parallelism) {
    return new ExtendedRecordConverter(parallelism);
  }

  /** @param inputPath path to directory with response files or a tar.xz archive */
  public long toAvro(String inputPath, DataFileWriter<ExtendedRecord> dataFileWriter) {
    if (Strings.isNullOrEmpty(inputPath)) {
      throw new ParsingException("Input or output stream must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);

    try (Stream<Path> walk = Files.walk(inputFile.toPath());
        UniquenessValidator validator = UniquenessValidator.getNewInstance()) {

      // Class with sync method to avoid problem with writing
      SyncDataFileWriter writer = new SyncDataFileWriter(dataFileWriter);

      AtomicLong counter = new AtomicLong(0);

      Predicate<Path> prefixPr = x -> x.toString().endsWith(FILE_PREFIX_RESPONSE) || x.toString().endsWith(FILE_PREFIX_XML);
      Function<File, ConverterTask> taskFn = f -> new ConverterTask(f, writer, validator, counter);

      // Run async process - read a file, convert to ExtendedRecord and write to avro
      CompletableFuture[] futures =
          walk.filter(x -> x.toFile().isFile() && prefixPr.test(x))
              .map(Path::toFile)
              .map(file -> CompletableFuture.runAsync(taskFn.apply(file), executor))
              .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();
      dataFileWriter.flush();

      return counter.get();

    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }
}
