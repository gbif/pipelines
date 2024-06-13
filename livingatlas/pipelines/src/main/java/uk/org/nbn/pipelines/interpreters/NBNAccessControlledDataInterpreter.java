package uk.org.nbn.pipelines.interpreters;

import au.org.ala.sds.generalise.FieldAccessor;
import java.util.*;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;
import uk.org.nbn.accesscontrol.DataResourceNbnCache;
import uk.org.nbn.dto.DataResourceNbn;
import uk.org.nbn.pipelines.io.avro.NBNAccessControlledRecord;
import uk.org.nbn.util.GeneralisedLocation;
import uk.org.nbn.util.GridUtil;
import uk.org.nbn.util.ScalaToJavaUtil;

/** Sensitive data interpretation methods. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class NBNAccessControlledDataInterpreter {
  protected static final TermFactory TERM_FACTORY = TermFactory.instance();

  protected static final FieldAccessor DATA_GENERALIZATIONS =
      new FieldAccessor(DwcTerm.dataGeneralizations);
  protected static final FieldAccessor INFORMATION_WITHHELD =
      new FieldAccessor(DwcTerm.informationWithheld);
  protected static final FieldAccessor GENERALISATION_TO_APPLY_IN_METRES =
      new FieldAccessor(TERM_FACTORY.findTerm("generalisationToApplyInMetres"));
  protected static final FieldAccessor GENERALISATION_IN_METRES =
      new FieldAccessor(TERM_FACTORY.findTerm("generalisationInMetres"));
  protected static final FieldAccessor DECIMAL_LATITUDE =
      new FieldAccessor(DwcTerm.decimalLatitude);
  protected static final FieldAccessor DECIMAL_LONGITUDE =
      new FieldAccessor(DwcTerm.decimalLongitude);
  protected static final double UNALTERED = 0.000001;

  /** Bits to skip when generically updating the temporal record */
  private static final Set<Term> SKIP_TEMPORAL_UPDATE = Collections.singleton(DwcTerm.eventDate);

  /**
   * Apply access control data changes to an AVRO location record.
   *
   * @param sr The access controlled record
   * @param locationRecord A location record
   */
  public static void applyAccessControls(
      NBNAccessControlledRecord sr, LocationRecord locationRecord) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) {
      return;
    }
    if (altered.containsKey("decimalLatitude")) {
      locationRecord.setDecimalLatitude(Double.parseDouble(altered.get("decimalLatitude")));
    }
    if (altered.containsKey("decimalLongitude")) {
      locationRecord.setDecimalLongitude(Double.parseDouble(altered.get("decimalLongitude")));
    }
    if (altered.containsKey("coordinateUncertaintyInMeters")) {
      locationRecord.setCoordinateUncertaintyInMeters(
          Double.parseDouble(altered.get("coordinateUncertaintyInMeters")));
    }
    if (altered.containsKey("locality")) {
      locationRecord.setLocality(altered.get("locality"));
    }
    if (altered.containsKey("footprintWKT")) {
      locationRecord.setFootprintWKT(altered.get("footprintWKT"));
    }
  }

  /**
   * TODO HMJ Apply access control data changes to an AVRO location record.
   *
   * @param sr The access controlled record
   * @param osGridRecord An OS grid record
   */
  //  public static void applyAccessControls(
  //          NBNAccessControlledRecord sr, OSGridRecord osGridRecord) {
  //    Map<String, String> altered = sr.getAltered();
  //
  //    if (altered == null || altered.isEmpty()) {
  //      return;
  //    }
  //    osGridRecord.setGridReference(altered.get("gridReference"));
  //    osGridRecord.setGridSizeInMeters(altered.get("gridSizeInMeters"));
  //
  //  }

  /**
   * Apply access control data changes to an AVRO location record.
   *
   * @param sr The access controlled record
   * @param extendedRecord An extended record
   */
  public static void applyAccessControls(
      NBNAccessControlledRecord sr, ExtendedRecord extendedRecord) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) {
      return;
    }
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.decimalLatitude.qualifiedName(), altered.get("decimalLatitude"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.decimalLongitude.qualifiedName(), altered.get("decimalLongitude"));
    extendedRecord
        .getCoreTerms()
        .put(
            DwcTerm.coordinateUncertaintyInMeters.qualifiedName(),
            altered.get("coordinateUncertaintyInMeters"));
    //    extendedRecord.getCoreTerms().put(DwcTerm.gridReference.qualifiedName(),
    // altered.get("gridReference"));
    //    extendedRecord.getCoreTerms().put(DwcTerm.gridSizeInMeters.qualifiedName(),
    // altered.get("gridSizeInMeters"));
    extendedRecord.getCoreTerms().put(DwcTerm.locality.qualifiedName(), altered.get("locality"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.verbatimLatitude.qualifiedName(), altered.get("verbatimLatitude"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.verbatimLongitude.qualifiedName(), altered.get("verbatimLongitude"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.verbatimLocality.qualifiedName(), altered.get("verbatimLocality"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.verbatimCoordinates.qualifiedName(), altered.get("verbatimCoordinates"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.footprintWKT.qualifiedName(), altered.get("footprintWKT"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.locationRemarks.qualifiedName(), altered.get("locationRemarks"));
    extendedRecord
        .getCoreTerms()
        .put(DwcTerm.occurrenceRemarks.qualifiedName(), altered.get("occurrenceRemarks"));
  }

  private static Map<String, String> blur(
      Map<String, String> original, int publicResolutionToBeApplied) {

    Map<String, String> blurred = new HashMap<>();

    if (original.get("decimalLatitude") != null && original.get("decimalLongitude") != null) {
      GeneralisedLocation generalisedLocation =
          new GeneralisedLocation(
              original.get("decimalLatitude"),
              original.get("decimalLongitude"),
              publicResolutionToBeApplied);
      blurred.put("decimalLatitude", generalisedLocation.getGeneralisedLatitude());
      blurred.put("decimalLongitude", generalisedLocation.getGeneralisedLongitude());
    }

    if (original.get("gridReference") != null && !original.get("gridReference").isEmpty()) {
      blurred.put(
          "gridReference",
          ScalaToJavaUtil.scalaOptionToString(
              GridUtil.convertReferenceToResolution(
                  original.get("gridReference"), String.valueOf(publicResolutionToBeApplied))));
    }

    String blurredCoordinateUncertainty =
        GridUtil.gridToCoordinateUncertaintyString(publicResolutionToBeApplied);

    if (original.get("coordinateUncertaintyInMeters") != null
        && !original.get("coordinateUncertaintyInMeters").isEmpty()
        && (java.lang.Double.parseDouble(original.get("coordinateUncertaintyInMeters"))
            < java.lang.Double.parseDouble(blurredCoordinateUncertainty))) {
      blurred.put("coordinateUncertaintyInMeters", blurredCoordinateUncertainty);
    }

    if (original.get("gridSizeInMeters") != null
        && !original.get("gridSizeInMeters").isEmpty()
        && java.lang.Integer.parseInt(original.get("coordinateUncertaintyInMeters"))
            < publicResolutionToBeApplied) {
      blurred.put("gridSizeInMeters", String.valueOf(publicResolutionToBeApplied));
    }

    // clear the remaining access controlled values
    blurred.put("locality", "");
    blurred.put("verbatimLatitude", "");
    blurred.put("verbatimLongitude", "");
    blurred.put("verbatimLocality", "");
    blurred.put("verbatimCoordinates", "");
    blurred.put("footprintWKT", "");
    blurred.put("locationRemarks", "");
    blurred.put("occurrenceRemarks", "");

    return blurred;
  }

  /**
   * Interprets a utils from the taxonomic properties supplied from the various source records.
   *
   * @param dataResourceUid The sensitive species lookup
   * @param accessControlledRecord The sensitive data report
   */
  public static void accessControlledDataInterpreter(
      String dataResourceUid,
      // NBNDataResourceService
      ExtendedRecord extendedRecord,
      LocationRecord locationRecord,
      // OSGridRecord osGridRecord,
      NBNAccessControlledRecord accessControlledRecord) {

    //TODO HMJ implement this using the ALA ws framework (which includes caching)
    DataResourceNbn dataResourceNbn =
        DataResourceNbnCache.getInstance().getDataResourceNbn(dataResourceUid);

    Integer publicResolutionToBeApplied =
        dataResourceNbn == null ? 10000 : dataResourceNbn.getPublicResolutionToBeApplied();

    accessControlledRecord.setAccessControlled(publicResolutionToBeApplied > 0);

    if (publicResolutionToBeApplied > 0) {

      Map<String, String> original = new HashMap<>();

      original.put(
          "decimalLatitude",
          locationRecord.getDecimalLatitude() != null
              ? locationRecord.getDecimalLatitude().toString()
              : null);
      original.put(
          "decimalLongitude",
          locationRecord.getDecimalLongitude() != null
              ? locationRecord.getDecimalLongitude().toString()
              : null);
      original.put(
          "coordinateUncertaintyInMeters",
          locationRecord.getCoordinateUncertaintyInMeters() != null
              ? locationRecord.getCoordinateUncertaintyInMeters().toString()
              : null);
      // original.put("gridReference", osGridRecord.getGridReference());
      // original.put("gridSizeInMeters", osGridRecord.getGridSizeInMeters());
      original.put("locality", locationRecord.getLocality());
      original.put(
          "verbatimLatitude",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimLatitude.qualifiedName()));
      original.put(
          "verbatimLongitude",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimLongitude.qualifiedName()));
      original.put(
          "verbatimLocality",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimLocality.qualifiedName()));
      original.put(
          "verbatimCoordinates",
          extendedRecord.getCoreTerms().get(DwcTerm.verbatimCoordinates.qualifiedName()));
      original.put("footprintWKT", locationRecord.getFootprintWKT());
      original.put(
          "locationRemarks",
          extendedRecord.getCoreTerms().get(DwcTerm.locationRemarks.qualifiedName()));
      original.put(
          "occurrenceRemarks",
          extendedRecord.getCoreTerms().get(DwcTerm.occurrenceRemarks.qualifiedName()));

      Map<String, String> blurred = blur(original, publicResolutionToBeApplied);

      // TODO this is not in phase1 so dont implement it yet
      //      accessControlledRecord.setDataGeneralizations(
      //              "Public resolution of "+dataResourceNbn.getPublicResolutionToBeApplied()+"m
      // applied");
      //      accessControlledRecord.setInformationWithheld(
      //
      // INFORMATION_WITHHELD.get(result).getValue().map(Object::toString).orElse(null));

      accessControlledRecord.setPublicResolutionInMetres(publicResolutionToBeApplied.toString());
      accessControlledRecord.setOriginal(toStringMap(original));
      accessControlledRecord.setAltered(toStringMap(blurred));
    }
  }

  /**
   * Add an issue to the issues list.
   *
   * @param sr The record
   * @param issue The issue
   */
  protected static void addIssue(NBNAccessControlledRecord sr, InterpretationRemark issue) {
    ModelUtils.addIssue(sr, issue.getId());
  }

  /** Convert a map into a map of string key-values. */
  protected static <K, V> Map<String, String> toStringMap(Map<K, V> original) {
    Map<String, String> strings = new HashMap<>(original.size());
    for (Map.Entry<K, V> entry : original.entrySet()) {
      strings.put(
          entry.getKey().toString(), entry.getValue() == null ? null : entry.getValue().toString());
    }
    return strings;
  }
}
