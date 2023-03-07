package org.gbif.dwca.validation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EmlNames {

  private static final String MATCH = " metadata=\"";

  public static Optional<Path> getEmlPath(Path directory) throws IOException {
    try (Stream<Path> stream =
        Files.find(directory, 1, (path, basicFileAttributes) -> path.toString().endsWith(".xml"))) {

      List<Path> xmlPaths = stream.collect(Collectors.toList());
      for (Path path : xmlPaths) {

        Optional<Path> first =
            Files.readAllLines(path).stream()
                .filter(lines -> lines.contains(MATCH))
                .findFirst()
                .map(str -> str.substring(str.indexOf(MATCH) + MATCH.length()))
                .map(str -> str.substring(0, str.indexOf("\"")))
                .map(directory::resolve);

        if (first.isPresent()) {
          return first;
        }
      }
      return Optional.empty();
    }
  }
}
