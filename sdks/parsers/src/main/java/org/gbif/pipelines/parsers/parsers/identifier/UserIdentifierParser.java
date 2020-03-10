package org.gbif.pipelines.parsers.parsers.identifier;

import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.gbif.api.vocabulary.UserIdentifierType;
import org.gbif.datarepo.api.validation.identifierschemes.OrcidValidator;
import org.gbif.datarepo.api.validation.identifierschemes.OtherValidator;
import org.gbif.pipelines.io.avro.UserIdentifier;

import com.google.common.base.Strings;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class UserIdentifierParser {

  private static final OrcidValidator ORCID_VALIDATOR = new OrcidValidator();
  private static final WikidataValidator WIKIDATA_VALIDATOR = new WikidataValidator();
  private static final OtherValidator OTHER_VALIDATOR = new OtherValidator();

  private static final String DELIMITER = "\\|";

  public static Set<UserIdentifier> parse(String raw) {
    if (Strings.isNullOrEmpty(raw)) {
      return Collections.emptySet();
    }
    return Stream.of(raw.split(DELIMITER))
        .map(String::trim)
        .map(UserIdentifierParser::parseValue)
        .collect(Collectors.toSet());
  }

  private static UserIdentifier parseValue(String raw) {
    if (ORCID_VALIDATOR.isValid(raw)) {
      return UserIdentifier.newBuilder()
          .setType(UserIdentifierType.ORCID.name())
          .setValue(ORCID_VALIDATOR.normalize(raw))
          .build();
    }
    if (WIKIDATA_VALIDATOR.isValid(raw)) {
      return UserIdentifier.newBuilder()
          .setType(UserIdentifierType.WIKIDATA.name())
          .setValue(WIKIDATA_VALIDATOR.normalize(raw))
          .build();
    }
    return UserIdentifier.newBuilder()
        .setType(UserIdentifierType.OTHER.name())
        .setValue(OTHER_VALIDATOR.normalize(raw))
        .build();
  }

}
