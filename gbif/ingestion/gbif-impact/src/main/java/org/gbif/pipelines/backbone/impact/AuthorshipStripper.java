package org.gbif.pipelines.backbone.impact;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.nameparser.NameParserGBIF;
import org.gbif.nameparser.api.NameParser;
import org.gbif.nameparser.api.ParsedName;
import org.gbif.nameparser.api.UnparsableNameException;

/** Removes the authorship from scientific names using the GBIF name parser. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AuthorshipStripper {

  // one parser per JVM, it is thread safe
  private static final NameParser PARSER = new NameParserGBIF();

  /**
   * @return the canonical name without authorship, or the name unchanged if it cannot be parsed
   */
  public static String strip(String name) {
    if (name == null || name.isBlank()) {
      return name;
    }
    try {
      ParsedName parsed = PARSER.parse(name);
      String canonical = parsed.canonicalNameWithoutAuthorship();
      return canonical == null || canonical.isBlank() ? name : canonical;
    } catch (UnparsableNameException e) {
      return name;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return name;
    }
  }
}
