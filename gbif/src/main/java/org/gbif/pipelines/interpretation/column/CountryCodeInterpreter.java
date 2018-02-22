package org.gbif.pipelines.interpretation.column;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.Collections;
import java.util.List;

/**
 * interprets country code
 */
class CountryCodeInterpreter implements Interpretable<String, String> {

  @Override
  public InterpretationResult<String> apply(String input) {
    ParseResult<Country> parseCountry = CountryParser.getInstance().parse(input.trim());
    if (parseCountry.isSuccessful()) {
      return InterpretationResult.withSuccess(parseCountry.getPayload().getIso3LetterCode());
    } else {
      List<Issue> issues = Collections.singletonList(Issue.newBuilder()
                                                       .setIssueType(IssueType.PARSE_ERROR)
                                                       .setRemark(parseCountry.getError() != null
                                                                    ? parseCountry.getError().getMessage()
                                                                    : "Could not parse countryCode")
                                                             .build());
      List<Lineage> lineages = Collections.singletonList(Lineage.newBuilder()
                                                                 .setLineageType(LineageType.SET_TO_NULL)
                                                                 .setRemark("Since the parse on countryCode failed, interpreting as null")
                                                           .build());
      return InterpretationResult.withIssueAndLineage(null, issues, lineages);
    }

  }
}
