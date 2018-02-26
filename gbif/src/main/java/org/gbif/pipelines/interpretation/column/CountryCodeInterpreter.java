package org.gbif.pipelines.interpretation.column;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

/**
 * interprets country code
 */
class CountryCodeInterpreter implements Interpretable<String, String> {

  @Override
  public InterpretationResult<String> apply(String input) {
    ParseResult<Country> parseCountry = CountryParser.getInstance().parse(input.trim());
    if (parseCountry.isSuccessful()) {
      return InterpretationResult.withSuccess(parseCountry.getPayload().getIso3LetterCode());
    }

    String issueText = parseCountry.getError() != null ? parseCountry.getError().getMessage() : "Could not parse countryCode";
    String lineageText = "Since the parse on countryCode failed, interpreting as null";
    return withIssueAndLineage(IssueType.PARSE_ERROR, issueText, LineageType.SET_TO_NULL, lineageText);
  }

}
