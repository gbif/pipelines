package org.gbif.pipelines.core.parsers.identifier;

import com.google.common.base.Strings;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.util.validators.identifierschemes.OrcidValidator;
import org.gbif.api.util.validators.identifierschemes.OtherValidator;
import org.gbif.api.util.validators.identifierschemes.WikidataValidator;
import org.gbif.api.vocabulary.AgentIdentifierType;
import org.gbif.pipelines.core.interpreters.model.AgentIdentifier;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class AgentIdentifierParser {

  private static final OrcidValidator ORCID_VALIDATOR = new OrcidValidator();
  private static final WikidataValidator WIKIDATA_VALIDATOR = new WikidataValidator();
  private static final OtherValidator OTHER_VALIDATOR = new OtherValidator();

  private static final String DELIMITER = "[|,]";

  public static Set<AgentIdentifier> parse(String raw) {
    if (Strings.isNullOrEmpty(raw)) {
      return Collections.emptySet();
    }
    return Stream.of(raw.split(DELIMITER))
        .map(String::trim)
        .filter(x -> !x.isEmpty())
        .map(AgentIdentifierParser::parseValue)
        .collect(Collectors.toSet());
  }

  private static AgentIdentifier parseValue(String raw) {
    if (ORCID_VALIDATOR.isValid(raw)) {
      return AgentIdentifier.builder()
          .type(AgentIdentifierType.ORCID.name())
          .value(ORCID_VALIDATOR.normalize(raw))
          .build();
    }
    if (WIKIDATA_VALIDATOR.isValid(raw)) {
      return AgentIdentifier.builder()
          .type(AgentIdentifierType.WIKIDATA.name())
          .value(WIKIDATA_VALIDATOR.normalize(raw))
          .build();
    }
    return AgentIdentifier.builder()
        .type(AgentIdentifierType.OTHER.name())
        .value(OTHER_VALIDATOR.normalize(raw))
        .build();
  }
}
