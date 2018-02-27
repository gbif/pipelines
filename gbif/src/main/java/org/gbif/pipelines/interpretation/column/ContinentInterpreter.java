package org.gbif.pipelines.interpretation.column;

import org.gbif.api.vocabulary.Continent;
import org.gbif.common.parsers.ContinentParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

/**
 * parse and interpret continents
 */
class ContinentInterpreter implements Interpretable<String, String> {

  @Override
  public InterpretationResult<String> apply(String input) {
    ParseResult<Continent> parse = ContinentParser.getInstance().parse(input);
    if (parse.isSuccessful()) {
      return InterpretationResult.withSuccess(parse.getPayload().getTitle());
    }

    String issueText = "Could not parse continent because " + (parse.getError() != null ? parse.getError().getMessage() : " is null");
    String lineageText = "Could not parse the continent or invalid value setting it to null";
    return withIssueAndLineage(IssueType.PARSE_ERROR, issueText, LineageType.SET_TO_NULL, lineageText);
  }
}
