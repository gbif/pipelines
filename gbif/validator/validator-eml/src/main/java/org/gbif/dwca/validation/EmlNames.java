package org.gbif.dwca.validation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class EmlNames {
  private static final Pattern PATTERN = Pattern.compile("(metadata=\")(\\w+.xml)(\")");

  public static Optional<Path> getEmlPath(Path directory) throws IOException {
    try (Stream<Path> stream =
        Files.find(directory, 1, (path, basicFileAttributes) -> path.toString().endsWith(".xml"))) {

      List<Path> xmlPaths = stream.collect(Collectors.toList());
      for (Path path : xmlPaths) {

        Optional<Path> first =
            Files.readAllLines(path).stream()
                .filter(lines -> PATTERN.matcher(lines).find())
                .findFirst()
                .map(
                    str -> {
                      Matcher matcher = PATTERN.matcher(str);
                      return matcher.find() ? matcher.group(2) : "";
                    })
                .map(directory::resolve);

        if (first.isPresent()) {
          return first;
        }
      }
      return Optional.empty();
    }
  }
}
