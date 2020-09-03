package org.gbif.pipelines.core.parsers.identifier;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.gbif.datarepo.api.validation.identifierschemes.IdentifierSchemeValidator;

public class WikidataValidator implements IdentifierSchemeValidator {

  private static final Pattern WIKIDATA_PATTERN =
      Pattern.compile(
          "^(?<scheme>(http(?:s)?:\\/\\/)?(www\\.))?(wikidata\\.org\\/\\w+\\/\\w+(?:\\:\\w+)?)$");

  @Override
  public boolean isValid(String value) {
    if (Strings.isNullOrEmpty(value)) {
      return false;
    }
    Matcher matcher = WIKIDATA_PATTERN.matcher(value);
    return matcher.matches();
  }

  @Override
  public String normalize(String value) {
    Preconditions.checkNotNull(value, "Identifier value can't be null");
    String trimmedValue = value.trim();
    Matcher matcher = WIKIDATA_PATTERN.matcher(trimmedValue);
    if (matcher.matches()) {
      return value;
    }
    throw new IllegalArgumentException(value + " it not a valid Wikidata");
  }
}
