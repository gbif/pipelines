package au.org.ala.pipelines.vocabulary;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.InterpretationRemarkSeverity;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.AnnotationUtils;

public enum ALAOccurrenceIssue implements InterpretationRemark {

  // Location related
  LOCATION_NOT_SUPPLIED(
      InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS_NO_DATUM),
  COORDINATES_CENTRE_OF_STATEPROVINCE(
      InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS_NO_DATUM),
  MISSING_COORDINATEPRECISION(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  UNCERTAINTY_IN_PRECISION(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  UNCERTAINTY_NOT_SPECIFIED(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  COORDINATE_PRECISION_INVALID(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),

  STATE_COORDINATE_MISMATCH(
      InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_COUNTRY_TERMS),
  UNKNOWN_COUNTRY_NAME(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_COUNTRY_TERMS),
  COORDINATES_CENTRE_OF_COUNTRY(
      InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_COUNTRY_TERMS),

  MISSING_GEODETICDATUM(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCE_DATE(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCEDBY(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCEPROTOCOL(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCESOURCES(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCEVERIFICATIONSTATUS(
      InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),

  // Temporal related
  MISSING_COLLECTION_DATE(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  GEOREFERENCE_POST_OCCURRENCE(
      InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  ID_PRE_OCCURRENCE(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  GEOREFERENCED_DATE_UNLIKELY(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),

  FIRST_OF_MONTH(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  FIRST_OF_YEAR(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  FIRST_OF_CENTURY(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),

  // Taxonomy related
  /** The scientific name has an aff. marker indicating a like-but-not identification */
  TAXON_AFFINITY_SPECIES(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The taxon, while matched, also has an excluded match */
  TAXON_EXCLUDED_ASSOCIATED(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The scientific name has a cf. marker, indicating uncertain identification */
  TAXON_CONFER_SPECIES(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The taxon is excluded (should not be found in this location) */
  TAXON_EXCLUDED(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** There has been a generic taxon matching error */
  TAXON_ERROR(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The supplied name and hierarchy is not enough to uniquely determine the name */
  TAXON_HOMONYM(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /**
   * The supplied name contains an indertminate species marker and an exact match could not be found
   */
  TAXON_INDETERMINATE_SPECIES(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The supplied name has been misapplied to another concept in the past */
  TAXON_MISAPPLIED_MATCHED(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The supplied name has been misapplied to another concept in the past with no accepted use */
  TAXON_MISAPPLIED(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The supplied name can be matched either onto the parent species or an autonymic subspecies */
  TAXON_PARENT_CHILD_SYNONYM(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The supplied name contains a questionable identification marker (?) */
  TAXON_QUESTION_SPECIES(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /**
   * The supplied name contains a species plural marker and cannot be accuratey matched onto a
   * single species
   */
  TAXON_SPECIES_PLURAL(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** There is a missing taxon rank */
  MISSING_TAXONRANK(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /**
   * Neither a scientific name or verncaulr name has been supplied; matching is based on a
   * constructed name
   */
  NAME_NOT_SUPPLIED(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /**
   * The supplied kingdom is not a recognised kingdom (and therefore not likely to help with
   * matching)
   */
  UNKNOWN_KINGDOM(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The supplied hints do not match the final name match */
  TAXON_SCOPE_MISMATCH(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The ocurrence record was matched to a default value supplied by higher-order elements */
  TAXON_DEFAULT_MATCH(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),
  /** The name is either unparsable or some type of placeholder or informal name */
  INVALID_SCIENTIFIC_NAME(InterpretationRemarkSeverity.WARNING, TermsGroup.TAXONOMY_TERMS),

  // Sensitive data related
  /** There is something wrong consturcting the sensitivity report */
  SENSITIVITY_REPORT_INVALID(InterpretationRemarkSeverity.ERROR, TermsGroup.SENSITIVE_DATA_TERMS),
  /** The sensitivity report cannot be applied to the record */
  SENSITIVITY_REPORT_NOT_LOADABLE(
      InterpretationRemarkSeverity.ERROR, TermsGroup.SENSITIVE_DATA_TERMS);

  private final Set<Term> relatedTerms;
  private final InterpretationRemarkSeverity severity;
  private final boolean isDeprecated;

  ALAOccurrenceIssue(InterpretationRemarkSeverity severity, Term[] relatedTerms) {
    this.severity = severity;
    this.relatedTerms = ImmutableSet.copyOf(relatedTerms);
    this.isDeprecated =
        AnnotationUtils.isFieldDeprecated(
            org.gbif.api.vocabulary.OccurrenceIssue.class, this.name());
  }

  @Override
  public String getId() {
    return this.name();
  }

  @Override
  public Set<Term> getRelatedTerms() {
    return this.relatedTerms;
  }

  @Override
  public InterpretationRemarkSeverity getSeverity() {
    return this.severity;
  }

  @Override
  public boolean isDeprecated() {
    return this.isDeprecated;
  }

  private static class TermsGroup {

    static final Term[] COORDINATES_TERMS_NO_DATUM;
    static final Term[] COORDINATES_TERMS;
    static final Term[] COUNTRY_TERMS;
    static final Term[] COORDINATES_COUNTRY_TERMS;
    static final Term[] RECORDED_DATE_TERMS;
    static final Term[] TAXONOMY_TERMS;
    static final Term[] GEOREFERENCE_TERMS;
    static final Term[] SENSITIVE_DATA_TERMS;

    private TermsGroup() {}

    static {
      COORDINATES_TERMS_NO_DATUM =
          new Term[] {
            DwcTerm.decimalLatitude,
            DwcTerm.decimalLongitude,
            DwcTerm.verbatimLatitude,
            DwcTerm.verbatimLongitude,
            DwcTerm.verbatimCoordinates
          };
      COORDINATES_TERMS =
          new Term[] {
            DwcTerm.decimalLatitude,
            DwcTerm.decimalLongitude,
            DwcTerm.verbatimLatitude,
            DwcTerm.verbatimLongitude,
            DwcTerm.verbatimCoordinates,
            DwcTerm.geodeticDatum
          };
      GEOREFERENCE_TERMS =
          new Term[] {
            DwcTerm.georeferencedBy,
            DwcTerm.georeferencedDate,
            DwcTerm.georeferencedBy,
            DwcTerm.georeferenceRemarks,
            DwcTerm.georeferenceProtocol,
            DwcTerm.georeferenceSources,
            DwcTerm.georeferenceVerificationStatus,
            DwcTerm.geodeticDatum
          };
      COUNTRY_TERMS = new Term[] {DwcTerm.country, DwcTerm.countryCode};
      COORDINATES_COUNTRY_TERMS =
          new Term[] {
            DwcTerm.decimalLatitude,
            DwcTerm.decimalLongitude,
            DwcTerm.verbatimLatitude,
            DwcTerm.verbatimLongitude,
            DwcTerm.verbatimCoordinates,
            DwcTerm.geodeticDatum,
            DwcTerm.country,
            DwcTerm.countryCode
          };
      RECORDED_DATE_TERMS =
          new Term[] {DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day};
      TAXONOMY_TERMS =
          new Term[] {
            DwcTerm.kingdom,
            DwcTerm.phylum,
            DwcTerm.class_,
            DwcTerm.order,
            DwcTerm.family,
            DwcTerm.genus,
            DwcTerm.scientificName,
            DwcTerm.scientificNameAuthorship,
            GbifTerm.genericName,
            DwcTerm.specificEpithet,
            DwcTerm.infraspecificEpithet,
            DwcTerm.taxonRank,
            DwcTerm.vernacularName
          };
      SENSITIVE_DATA_TERMS =
          new Term[] {
            DwcTerm.scientificName,
            DwcTerm.taxonConceptID,
            DwcTerm.decimalLatitude,
            DwcTerm.decimalLongitude,
            DwcTerm.eventDate
          };
    }
  }
}
