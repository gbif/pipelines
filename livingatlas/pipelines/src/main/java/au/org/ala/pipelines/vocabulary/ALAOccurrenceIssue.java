package au.org.ala.pipelines.vocabulary;

import com.google.common.collect.ImmutableSet;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.InterpretationRemarkSeverity;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.GbifTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.AnnotationUtils;
import java.util.Set;

public enum ALAOccurrenceIssue implements InterpretationRemark {

  //Location related
  LOCATION_NOT_SUPPLIED(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS_NO_DATUM),
  COORDINATES_CENTRE_OF_STATEPROVINCE(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS_NO_DATUM),
  MISSING_COORDINATEPRECISION(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  UNCERTAINTY_IN_PRECISION(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  UNCERTAINTY_NOT_SPECIFIED(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  COORDINATE_PRECISION_INVALID(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),
  COORDINATE_PRECISION_MISMATCH(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_TERMS),

  STATE_COORDINATE_MISMATCH(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_COUNTRY_TERMS),
  UNKNOWN_COUNTRY_NAME(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_COUNTRY_TERMS),
  COORDINATES_CENTRE_OF_COUNTRY(InterpretationRemarkSeverity.WARNING, TermsGroup.COORDINATES_COUNTRY_TERMS),

  MISSING_GEODETICDATUM(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCE_DATE(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCEDBY(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCEPROTOCOL(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCESOURCES(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),
  MISSING_GEOREFERENCEVERIFICATIONSTATUS(InterpretationRemarkSeverity.WARNING, TermsGroup.GEOREFERENCE_TERMS),

  //Temporal related
  MISSING_COLLECTION_DATE(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  GEOREFERENCE_POST_OCCURRENCE(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  ID_PRE_OCCURRENCE(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  GEOREFERENCED_DATE_UNLIKELY(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),

  FIRST_OF_MONTH(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  FIRST_OF_YEAR(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS),
  FIRST_OF_CENTURY(InterpretationRemarkSeverity.WARNING, TermsGroup.RECORDED_DATE_TERMS);

  private final Set<Term> relatedTerms;
  private final InterpretationRemarkSeverity severity;
  private final boolean isDeprecated;

  ALAOccurrenceIssue(InterpretationRemarkSeverity severity, Term[] relatedTerms) {
    this.severity = severity;
    this.relatedTerms = ImmutableSet.copyOf(relatedTerms);
    this.isDeprecated = AnnotationUtils
        .isFieldDeprecated(org.gbif.api.vocabulary.OccurrenceIssue.class, this.name());
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

    private TermsGroup() {
    }

    static {
      COORDINATES_TERMS_NO_DATUM = new Term[]{DwcTerm.decimalLatitude, DwcTerm.decimalLongitude,
          DwcTerm.verbatimLatitude, DwcTerm.verbatimLongitude, DwcTerm.verbatimCoordinates};
      COORDINATES_TERMS = new Term[]{DwcTerm.decimalLatitude, DwcTerm.decimalLongitude,
          DwcTerm.verbatimLatitude, DwcTerm.verbatimLongitude, DwcTerm.verbatimCoordinates,
          DwcTerm.geodeticDatum};
      GEOREFERENCE_TERMS = new Term[]{DwcTerm.georeferencedBy, DwcTerm.georeferencedDate,
          DwcTerm.georeferencedBy, DwcTerm.georeferenceRemarks, DwcTerm.georeferenceProtocol,
          DwcTerm.georeferenceSources, DwcTerm.georeferenceVerificationStatus,
          DwcTerm.geodeticDatum};
      COUNTRY_TERMS = new Term[]{DwcTerm.country, DwcTerm.countryCode};
      COORDINATES_COUNTRY_TERMS = new Term[]{DwcTerm.decimalLatitude, DwcTerm.decimalLongitude,
          DwcTerm.verbatimLatitude, DwcTerm.verbatimLongitude, DwcTerm.verbatimCoordinates,
          DwcTerm.geodeticDatum, DwcTerm.country, DwcTerm.countryCode};
      RECORDED_DATE_TERMS = new Term[]{DwcTerm.eventDate, DwcTerm.year, DwcTerm.month, DwcTerm.day};
      TAXONOMY_TERMS = new Term[]{DwcTerm.kingdom, DwcTerm.phylum, DwcTerm.class_, DwcTerm.order,
          DwcTerm.family, DwcTerm.genus, DwcTerm.scientificName, DwcTerm.scientificNameAuthorship,
          GbifTerm.genericName, DwcTerm.specificEpithet, DwcTerm.infraspecificEpithet};
    }
  }
}
