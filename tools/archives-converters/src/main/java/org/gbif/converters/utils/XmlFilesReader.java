package org.gbif.converters.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class XmlFilesReader {

  private static final String FILE_PREFIX_RESPONSE = ".response";
  private static final String FILE_PREFIX_XML = ".xml";

  /** Traverse the input directory and gets all the files. */
  public static List<File> getInputFiles(File inputhFile) throws IOException {
    Predicate<Path> prefixPr =
        x -> x.toString().endsWith(FILE_PREFIX_RESPONSE) || x.toString().endsWith(FILE_PREFIX_XML);
    try (Stream<Path> walk =
        Files.walk(inputhFile.toPath())
            .filter(file -> file.toFile().isFile() && prefixPr.test(file))) {
      return walk.map(Path::toFile).collect(Collectors.toList());
    }
  }
}
