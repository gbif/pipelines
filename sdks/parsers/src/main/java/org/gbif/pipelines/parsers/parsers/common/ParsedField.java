package org.gbif.pipelines.parsers.parsers.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import lombok.Builder;
import lombok.Getter;

/**
 * Models a parsed field.
 *
 * <p>A field can be as simple as a single {@link org.gbif.dwc.terms.DwcTerm} or a wrapper with
 * several fields inside.
 */
@Builder
@Getter
public class ParsedField<T> {

  private final T result;

  @Builder.Default
  private final List<String> issues = new ArrayList<>();

  private final boolean successful;

  public static <S> ParsedField<S> fail(S result, List<String> issues) {
    return ParsedField.<S>builder().result(result).issues(issues).build();
  }

  public static <S> ParsedField<S> fail(String issue) {
    return ParsedField.<S>builder().issues(Collections.singletonList(issue)).build();
  }

  public static <S> ParsedField<S> fail(List<String> issues) {
    return ParsedField.<S>builder().issues(issues).build();
  }

  public static <S> ParsedField<S> fail() {
    return ParsedField.<S>builder().build();
  }

  public static <S> ParsedField<S> success(S result, List<String> issues) {
    return ParsedField.<S>builder().successful(true).result(result).issues(issues).build();
  }

  public static <S> ParsedField<S> success(S result) {
    return ParsedField.<S>builder().successful(true).result(result).build();
  }

}
