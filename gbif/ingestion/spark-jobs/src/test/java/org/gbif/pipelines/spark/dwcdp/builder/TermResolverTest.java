package org.gbif.pipelines.spark.dwcdp.builder;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.stream.Stream;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifDnaTerm;
import org.gbif.dwc.terms.GgbnTerm;
import org.gbif.dwc.terms.MixsTerm;
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
        Arguments.of("mediaType", DcTerm.type.qualifiedName()),
        // nucleotide-sequence.sequence resolves via TermFactory, but to the real-yet-wrong
        // ggbn:sequence rather than the gbif:dna_sequence the DNA Derived Data interpreter
        // actually reads — same "resolves, just to the wrong term" shape as accessURI.
        Arguments.of("sequence", GbifDnaTerm.dna_sequence.qualifiedName()),
        // molecular-protocol uses DwC-DP's own naming here rather than MixsTerm's abbreviated
        // sc_lysis_approach/sc_lysis_method constants — TermFactory has no matching simple name
        // for either DwC-DP spelling at all.
        Arguments.of("single_cell_lysis_appr", MixsTerm.sc_lysis_approach.qualifiedName()),
        Arguments.of("single_cell_lysis_prot", MixsTerm.sc_lysis_method.qualifiedName()));
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

  static Stream<Arguments> samplingAndGeoreferenceProtocolCases() {
    return Stream.of(
        Arguments.of("samplingProtocol", DwcTerm.samplingProtocol.qualifiedName()),
        Arguments.of("georeferenceProtocol", DwcTerm.georeferenceProtocol.qualifiedName()));
  }

  @ParameterizedTest(name = "{0} → {1}")
  @MethodSource("samplingAndGeoreferenceProtocolCases")
  void termFactory_resolvesProtocolColumnsToQualifiedUri(String input, String expected) {
    assertEquals(expected, TermResolver.resolve(input));
  }

  static Stream<Arguments> provenanceAttributionCases() {
    return Stream.of(
        Arguments.of("fundingAttribution", DwcTerm.fundingAttribution.qualifiedName()),
        Arguments.of("fundingAttributionID", DwcTerm.fundingAttributionID.qualifiedName()),
        Arguments.of("projectID", DwcTerm.projectID.qualifiedName()),
        Arguments.of("projectTitle", DwcTerm.projectTitle.qualifiedName()));
  }

  @ParameterizedTest(name = "{0} → {1}")
  @MethodSource("provenanceAttributionCases")
  void termFactory_resolvesProvenanceAttributionFieldsToQualifiedUri(
      String input, String expected) {
    assertEquals(expected, TermResolver.resolve(input));
  }

  /**
   * {@code molecular-protocol} column names that match a registered {@code MixsTerm}/{@code
   * GgbnTerm} simple name exactly and are expected to already resolve correctly with no {@link
   * DwcDpTermMappings} entry — confirmed against the dwc-api javadoc's enumerated constants (see
   * {@link org.gbif.pipelines.spark.dwcdp.builder.extension.NucleotideExtensionBuilder}'s class
   * docs) rather than assumed. {@code single_cell_lysis_appr}/{@code single_cell_lysis_prot} are
   * deliberately absent here — they're the confirmed exceptions, covered by {@link #renameCases()}
   * instead.
   */
  static Stream<Arguments> molecularProtocolTermFactoryCases() {
    return Stream.of(
        Arguments.of("target_gene", MixsTerm.target_gene.qualifiedName()),
        Arguments.of("pcr_primers", MixsTerm.pcr_primers.qualifiedName()),
        Arguments.of("seq_meth", MixsTerm.seq_meth.qualifiedName()),
        Arguments.of("nucl_acid_amp", MixsTerm.nucl_acid_amp.qualifiedName()),
        Arguments.of("assembly_qual", MixsTerm.assembly_qual.qualifiedName()),
        Arguments.of("otu_db", MixsTerm.otu_db.qualifiedName()),
        Arguments.of("pcr_primer_forward", GbifDnaTerm.pcr_primer_forward.qualifiedName()),
        Arguments.of("pcr_primer_reverse", GbifDnaTerm.pcr_primer_reverse.qualifiedName()),
        Arguments.of("concentration", GgbnTerm.concentration.qualifiedName()),
        Arguments.of("ratioOfAbsorbance260_230", GgbnTerm.ratioOfAbsorbance260_230.qualifiedName()),
        Arguments.of(
            "methodDeterminationConcentrationAndRatios",
            GgbnTerm.methodDeterminationConcentrationAndRatios.qualifiedName()));
  }

  @ParameterizedTest(name = "{0} → {1}")
  @MethodSource("molecularProtocolTermFactoryCases")
  void termFactory_resolvesMolecularProtocolFieldsToQualifiedUri(String input, String expected) {
    assertEquals(expected, TermResolver.resolve(input));
  }
}
