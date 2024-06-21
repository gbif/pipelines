package uk.org.nbn.pipelines.vocabulary;

import com.google.common.collect.ImmutableSet;
import java.util.Set;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.api.vocabulary.InterpretationRemarkSeverity;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.utils.AnnotationUtils;
import uk.org.nbn.term.OSGridTerm;

/**
 * Living Atlas issue extension. We should look to merge this with {@link
 * org.gbif.api.vocabulary.OccurrenceIssue} enum See: https://github.com/gbif/pipelines/issues/530
 */
public enum NBNOccurrenceIssue implements InterpretationRemark {

  // Location related
  DECIMAL_LAT_LONG_CALCULATED_FROM_GRID_REF(
      InterpretationRemarkSeverity.INFO,
      new Term[] {DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, OSGridTerm.gridReference}),
  DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_FAILED(
      InterpretationRemarkSeverity.WARNING,
      new Term[] {OSGridTerm.easting, OSGridTerm.northing, OSGridTerm.zone}),
  DECIMAL_LAT_LONG_CALCULATED_FROM_EASTING_NORTHING(
      InterpretationRemarkSeverity.INFO,
      new Term[] {OSGridTerm.easting, OSGridTerm.northing, OSGridTerm.zone}),

  DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_UNRECOGNISED_GDA94_ZONE(
      InterpretationRemarkSeverity.WARNING, new Term[] {DwcTerm.verbatimSRS, OSGridTerm.zone}),

  DECIMAL_LAT_LONG_CALCULATION_FROM_EASTING_NORTHING_UNRECOGNISED_VERBATIMSRS_ZONE(
      InterpretationRemarkSeverity.WARNING, new Term[] {DwcTerm.verbatimSRS, OSGridTerm.zone}),

  COORDINATES_NOT_CENTRE_OF_GRID(
      InterpretationRemarkSeverity.INFO,
      new Term[] {DwcTerm.decimalLatitude, DwcTerm.decimalLongitude, OSGridTerm.gridReference}),

  GRID_REF_CALCULATED_FROM_LAT_LONG(
      InterpretationRemarkSeverity.INFO, new Term[] {OSGridTerm.gridReference}),

  MISSING_IDENTIFICATIONVERIFICATIONSTATUS(
      InterpretationRemarkSeverity.ERROR, new Term[] {DwcTerm.identificationVerificationStatus}),

  UNRECOGNISED_IDENTIFICATIONVERIFICATIONSTATUS(
      InterpretationRemarkSeverity.ERROR, new Term[] {DwcTerm.identificationVerificationStatus});

  private final Set<Term> relatedTerms;
  private final InterpretationRemarkSeverity severity;
  private final boolean isDeprecated;

  NBNOccurrenceIssue(InterpretationRemarkSeverity severity, Term[] relatedTerms) {
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

    static final Term[] OSGRID_TERMS;

    private TermsGroup() {}

    static {
      OSGRID_TERMS =
          new Term[] {
            OSGridTerm.gridReference, OSGridTerm.easting, OSGridTerm.northing, OSGridTerm.zone
          };
    }
  }
}
