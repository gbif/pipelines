package org.gbif.pipelines.parsers.parsers.identifier;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gbif.datarepo.api.validation.identifierschemes.IdentifierSchemeValidator;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

public class WikidataValidator implements IdentifierSchemeValidator {

  private static final Pattern WIKIDATA_PATTERN =
      Pattern.compile("^(?<scheme>(http(?:s)?:\\/\\/)?(www\\.))?(wikidata\\.org\\/\\w+\\/\\w+(?:\\:\\w+)?)$");

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
      return "https://www." + withoutScheme(matcher, trimmedValue);
    }
    throw new IllegalArgumentException(value + " it not a valid Wikidata");
  }

  private static String withoutScheme(Matcher matcher, String value) {
    String schemeGroup = matcher.group("scheme");
    return schemeGroup == null ? value : value.substring(schemeGroup.length());
  }
}
