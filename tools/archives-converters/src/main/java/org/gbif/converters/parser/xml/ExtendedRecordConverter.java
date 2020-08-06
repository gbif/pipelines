package org.gbif.converters.parser.xml;

import com.google.common.base.Strings;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.converters.converter.SyncDataFileWriter;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ConverterTask;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ExecutorPoolFactory;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.converters.parser.xml.parsing.validators.UniquenessValidator;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/** Parsing xml response files or tar.xz archive and convert to ExtendedRecord avro file */
@Slf4j
@AllArgsConstructor(staticName = "create")
public class ExtendedRecordConverter {

  private static final String FILE_PREFIX_RESPONSE = ".response";
  private static final String FILE_PREFIX_XML = ".xml";

  private final Executor executor;

  private ExtendedRecordConverter(int parallelism) {
    this.executor = ExecutorPoolFactory.getInstance(parallelism);
  }

  public static ExtendedRecordConverter create(int parallelism) {
    return new ExtendedRecordConverter(parallelism);
  }

  /** @param inputPath path to directory with response files or a tar.xz archive */
  public long toAvro(String inputPath, SyncDataFileWriter<ExtendedRecord> writer) {
    if (Strings.isNullOrEmpty(inputPath)) {
      throw new ParsingException("Input or output stream must not be empty or null!");
    }

    File inputFile = ParserFileUtils.uncompressAndGetInputFile(inputPath);

    try (UniquenessValidator validator = UniquenessValidator.getNewInstance()) {
      List<File> files = getInputFiles(inputFile);

      AtomicLong counter = new AtomicLong(0);

      Function<File, ConverterTask> taskFn = f -> new ConverterTask(f, writer, validator, counter);

      // Run async process - read a file, convert to ExtendedRecord and write to Avro
      CompletableFuture[] futures =
          files.stream()
              .map(file -> CompletableFuture.runAsync(taskFn.apply(file), executor))
              .toArray(CompletableFuture[]::new);

      // Wait all threads
      CompletableFuture.allOf(futures).get();

      return counter.get();

    } catch (Exception ex) {
      log.error(ex.getMessage(), ex);
      throw new ParsingException(ex);
    }
  }

  /** Traverse the input directory and gets all the files. */
  private List<File> getInputFiles(File inputhFile) throws IOException {
    Predicate<Path> prefixPr =
        x -> x.toString().endsWith(FILE_PREFIX_RESPONSE) || x.toString().endsWith(FILE_PREFIX_XML);
    try (Stream<Path> walk =
        Files.walk(inputhFile.toPath())
            .filter(file -> file.toFile().isFile() && prefixPr.test(file))) {
      return walk.map(Path::toFile).collect(Collectors.toList());
    }
  }
}
