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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

/** Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file */
public class ExtendedRecordConverter {

  private static final Logger LOG = LoggerFactory.getLogger(ExtendedRecordConverter.class);

  private static final String FILE_PREFIX_RESPONSE = ".response";
  private static final String FILE_PREFIX_XML = ".xml";

  private final Executor executor;
  private final String idHashPrefix;

  private ExtendedRecordConverter(int parallelism, String idHashPrefix) {
    this.executor = ExecutorPool.getInstance(parallelism);
    this.idHashPrefix = idHashPrefix;
  }

  public static ExtendedRecordConverter crete(int parallelism) {
    return new ExtendedRecordConverter(parallelism, null);
  }

  public static ExtendedRecordConverter crete(int parallelism, String idHashPrefix) {
    return new ExtendedRecordConverter(parallelism, idHashPrefix);
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
      Function<File, ConverterTask> taskFn = f -> new ConverterTask(f, writer, validator, counter, idHashPrefix);

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
      LOG.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }
}
