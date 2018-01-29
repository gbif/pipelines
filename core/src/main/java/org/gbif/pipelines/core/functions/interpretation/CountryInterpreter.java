package org.gbif.pipelines.core.functions.interpretation;

import org.gbif.api.vocabulary.Country;
import org.gbif.common.parsers.CountryParser;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.pipelines.core.functions.interpretation.error.Issue;
import org.gbif.pipelines.core.functions.interpretation.error.IssueType;
import org.gbif.pipelines.core.functions.interpretation.error.Lineage;
import org.gbif.pipelines.core.functions.interpretation.error.LineageType;

import java.util.ArrayList;
import java.util.List;

/**
 * interprets country
 */
public class CountryInterpreter implements Interpretable<String, String> {

  @Override
  public String interpret(String input) throws InterpretationException {
    List<Issue> issues = new ArrayList<>();
    List<Lineage> lineages = new ArrayList<>();

    ParseResult<Country> parseCountry = CountryParser.getInstance().parse(input.trim());
    if (parseCountry.isSuccessful()) {
      return parseCountry.getPayload().getTitle();
    } else {
      issues.add(Issue.newBuilder()
                   .setIssueType(IssueType.PARSE_ERROR)
                   .setRemark(parseCountry.getError().getMessage())
                   .build());
      lineages.add(Lineage.newBuilder()
                     .setLineageType(LineageType.SET_TO_NULL)
                     .setRemark("Since the parse on country failed, interpreting as null")
                     .build());

    }
    InterpretationException iex = new InterpretationException(issues, lineages);
    iex.setInterpretedValue(null);
    throw iex;
  }
}
