package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Parameterized tests for {@link TermResolver}, one method per resolution scenario.
 *
 * <p>No Spark session needed — pure unit tests.
 */
class TermResolverTest {

  /**
   * DwC-DP field names that differ from their DwC-A equivalents and are not registered as
   * alternatives in TermFactory. Each must resolve to the correct qualified URI via {@link
   * DwcDpTermMappings#RENAMES}.
   */
  static Stream<Arguments> renameCases() {
    return Stream.of(
        Arguments.of("occurrenceReferences", DwcTerm.associatedReferences.qualifiedName()),
        Arguments.of("eventConductedBy", DwcTerm.recordedBy.qualifiedName()),
        Arguments.of("eventConductedByID", DwcTerm.recordedByID.qualifiedName()),
        Arguments.of("accessURI", DcTerm.identifier.qualifiedName()),
        Arguments.of("mediaType", DcTerm.type.qualifiedName()));
  }

  @ParameterizedTest(name = "{0} → {1}")
  @MethodSource("renameCases")
  void renameMap_resolvesToCorrectQualifiedUri(String input, String expected) {
    assertEquals(expected, TermResolver.resolve(input));
  }

  /** Standard DwC terms that TermFactory resolves directly from the dwc-api library. */
  static Stream<Arguments> termFactoryCases() {
    return Stream.of(
        Arguments.of("eventID", DwcTerm.eventID.qualifiedName()),
        Arguments.of("scientificName", DwcTerm.scientificName.qualifiedName()),
        Arguments.of("occurrenceID", DwcTerm.occurrenceID.qualifiedName()),
        Arguments.of("organismID", DwcTerm.organismID.qualifiedName()),
        Arguments.of("organismName", DwcTerm.organismName.qualifiedName()),
        Arguments.of("organismScope", DwcTerm.organismScope.qualifiedName()),
        Arguments.of("associatedOrganisms", DwcTerm.associatedOrganisms.qualifiedName()),
        Arguments.of("previousIdentifications", DwcTerm.previousIdentifications.qualifiedName()),
        Arguments.of("organismRemarks", DwcTerm.organismRemarks.qualifiedName()));
  }

  @ParameterizedTest(name = "{0} → {1}")
  @MethodSource("termFactoryCases")
  void termFactory_resolvesToQualifiedUri(String input, String expected) {
    assertEquals(expected, TermResolver.resolve(input));
    assertTrue(
        TermResolver.resolve(input).startsWith("http://rs.tdwg.org/dwc/terms/"),
        "Expected dwc namespace URI for: " + input);
  }

  /**
   * Terms that cannot be resolved — either not yet in dwc-api, or genuinely non-standard. Expected
   * value is the raw column name.
   *
   * <p>When dwc-api is upgraded and a term (e.g. causeOfDeath) is added to {@link DwcTerm}, its
   * entry here will fail because TermFactory will resolve it to a qualified URI. At that point:
   * remove it from here and add it to {@link #termFactoryCases()}.
   */
  static Stream<Arguments> fallThroughCases() {
    return Stream.of(
        // in DwC 2025+ standard but not yet in dwc-api
        Arguments.of("causeOfDeath"),
        Arguments.of("substrate"),
        Arguments.of("feedbackURL"),
        // genuinely non-standard publisher column
        Arguments.of("somePublisherSpecificColumn"));
  }

  @ParameterizedTest(name = "{0} falls through to raw name")
  @MethodSource("fallThroughCases")
  void unresolvable_fallsThroughToRawColumnName(String input) {
    assertEquals(input, TermResolver.resolve(input));
  }

  /**
   * DwC-DP media field names that are expected to already resolve correctly via TermFactory with no
   * rename needed, because they match their target DcTerm's simple name exactly. Confirming this in
   * a test rather than assuming it — unlike accessURI/mediaType above, these were never verified
   * against the actual TermFactory behavior, only assumed to "probably just work."
   */
  static Stream<Arguments> dcTermFactoryCases() {
    return Stream.of(
        Arguments.of("format", DcTerm.format.qualifiedName()),
        Arguments.of("title", DcTerm.title.qualifiedName()),
        Arguments.of("description", DcTerm.description.qualifiedName()));
  }

  @ParameterizedTest(name = "{0} → {1}")
  @MethodSource("dcTermFactoryCases")
  void termFactory_resolvesDcTermsToQualifiedUri(String input, String expected) {
    assertEquals(expected, TermResolver.resolve(input));
  }
}
