package org.gbif.pipelines.fragmenter.strategy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.gbif.converters.parser.xml.OccurrenceParser;
import org.gbif.converters.parser.xml.parsing.extendedrecord.ParserFileUtils;
import org.gbif.pipelines.fragmenter.record.OccurrenceRecord;
import org.gbif.pipelines.fragmenter.record.XmlOccurrenceRecord;

/** Processing strategy for XML based archives */
@NoArgsConstructor(staticName = "create")
public class XmlStrategy implements Strategy {

  private static final String EXT_RESPONSE = ".response";
  private static final String EXT_XML = ".xml";

  @SneakyThrows
  @Override
  public void process(Path path, Consumer<OccurrenceRecord> pushRecordFn) {
    File inputFile = ParserFileUtils.uncompressAndGetInputFile(path.toString());
    getInputFiles(inputFile)
        .forEach(
            f ->
                OccurrenceParser.parse(f).stream()
                    .map(XmlOccurrenceRecord::create)
                    .forEach(pushRecordFn));
  }

  /** Traverse the input directory and gets all the files. */
  private List<File> getInputFiles(File inputhFile) throws IOException {
    Predicate<Path> prefixPr =
        x -> x.toString().endsWith(EXT_RESPONSE) || x.toString().endsWith(EXT_XML);
    try (Stream<Path> walk =
        Files.walk(inputhFile.toPath())
            .filter(file -> file.toFile().isFile() && prefixPr.test(file))) {
      return walk.map(Path::toFile).collect(Collectors.toList());
    }
  }
}
