package org.gbif.dwca.validation;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetadataPath {

  private static final String DEFAULT_NAME = "eml.xml";
  private static final String DEFAULT_NAME_UPPER = "metadata.xml";
  private static final Pattern PATTERN =
      Pattern.compile("(metadata=[\"'])([^\"']+)([\"'])", Pattern.CASE_INSENSITIVE);

  /**
   * Attempt to read the DWCA meta.xml file for the metadata location, but otherwise try the two
   * defaults used by the dwca-io library.
   */
  public static Optional<Path> parsePath(Path rootDirectory) throws IOException {
    // Look for metadata at the location specified in meta.xml, and at eml.xml and metadata.xml.
    Path metaXml = rootDirectory.resolve("meta.xml");
    if (Files.exists(metaXml)) {

      Optional<Path> fromMetaXml =
          Files.readAllLines(metaXml).stream()
              .filter(lines -> PATTERN.matcher(lines).find())
              .findFirst()
              .map(
                  str -> {
                    Matcher matcher = PATTERN.matcher(str);
                    return matcher.find() ? matcher.group(2) : "";
                  })
              .map(rootDirectory::resolve);

      if (fromMetaXml.isPresent()) {
        return fromMetaXml;
      }
    }

    Path defaultNamePath = rootDirectory.resolve(DEFAULT_NAME);
    if (Files.exists(defaultNamePath)) {
      return Optional.of(defaultNamePath);
    }

    Path defaultUpperNamePath = rootDirectory.resolve(DEFAULT_NAME_UPPER);
    if (Files.exists(defaultUpperNamePath)) {
      return Optional.of(defaultUpperNamePath);
    }

    return Optional.empty();
  }
}
