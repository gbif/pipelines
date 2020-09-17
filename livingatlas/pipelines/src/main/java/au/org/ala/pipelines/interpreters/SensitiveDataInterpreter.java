package au.org.ala.pipelines.interpreters;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
import au.org.ala.sds.api.SpeciesCheck;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.OccurrenceHdfsRecord;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.parsers.utils.ModelUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SensitiveDataInterpreter {
  private static String ORIGINAL_VALUES = "originalSensitiveValues";
  private static String DATA_GENERALIZATIONS = "dataGeneralizations";
  private static String GENERALISATION_TO_APPLY_IN_METRES = "generalisationToApplyInMetres";
  private static String GENERALISATION_IN_METRES = "generalisationInMetres";

  private static Set<String> TAXON_ROWS =
      new HashSet<String>(
          Arrays.asList(
              ALATaxonRecord.getClassSchema().getName(),
              OccurrenceHdfsRecord.getClassSchema().getName()));

  /**
   * Construct information from the extended record.
   *
   * <p>If there is any extension with this value, that takes precedence. Otherwise, the core record
   * is used.
   *
   * <p>If a field is already set, the current value is ignored.
   *
   * @param sensitiveFields The list of sensitive fields
   * @param fields The field map that we are constructing
   * @param er The extended record
   */
  public static void constructFields(
      Set<String> sensitiveFields, Map<String, String> fields, ExtendedRecord er) {
    if (er == null) return;
    er.getExtensions()
        .values()
        .forEach(el -> el.forEach(ext -> constructFields(sensitiveFields, fields, ext)));
    constructFields(sensitiveFields, fields, er.getCoreTerms());
  }

  /**
   * Construct information from an AVRO record
   *
   * <p>If a field is already set, the current value is ignored.
   *
   * @param sensitiveFields The list of sensitive fields
   * @param fields The field map that we are constructing
   * @param record A record
   */
  public static void constructFields(
      Set<String> sensitiveFields, Map<String, String> fields, IndexedRecord record) {
    int i = 0;

    if (record == null) return;
    for (Schema.Field f : record.getSchema().getFields()) {
      String name = f.name();
      if (sensitiveFields.contains(name) && !fields.containsKey(name)) {
        Object value = record.get(i);
        fields.put(name, value == null ? null : value.toString());
      }
      i++;
    }
  }

  protected static void constructFields(
      Set<String> sensitiveFields, Map<String, String> fields, Map<String, String> values) {
    values
        .entrySet()
        .forEach(
            e -> {
              if (sensitiveFields.contains(e.getKey()) && !fields.containsKey(e.getKey()))
                fields.put(e.getKey(), e.getValue());
            });
  }

  /**
   * Apply sensitive data changes to an AVRO record.
   *
   * @param sr The sensitivity record
   * @param record A record
   */
  public static void applySensitivity(ALASensitivityRecord sr, IndexedRecord record) {
    Map<String, String> altered = sr.getAltered();
    int i = 0;

    if (altered == null || altered.isEmpty()) return;
    for (Schema.Field f : record.getSchema().getFields()) {
      String name = f.name();
      if (altered.containsKey(name)) {
        record.put(i, altered.get(name));
      }
      i++;
    }
  }

  /**
   * Apply sensitive data changes to a verbatim record.
   *
   * @param sr The sensitivity record
   * @param record A record
   */
  public static void applySensitivity(ALASensitivityRecord sr, ExtendedRecord record) {
    record.getExtensions().forEach((k, el) -> el.forEach(ext -> applySensitivity(sr, ext)));
    applySensitivity(sr, record.getCoreTerms());
  }

  protected static void applySensitivity(ALASensitivityRecord sr, Map<String, String> values) {
    Map<String, String> altered = sr.getAltered();
    values.forEach(
        (k, v) -> {
          if (values.containsKey(k)) values.put(k, altered.get(k));
        });
  }

  /**
   * Perform quality checks on the incoming record.
   *
   * @param properties The potentially sensitive occurrence properties
   * @param sr The sensitivity record
   * @param dataResource The associated data resource.
   * @return True if we have enough to be going on with
   */
  public static boolean sourceQualityChecks(
      final Map<String, String> properties,
      final ALASensitivityRecord sr,
      final ALACollectoryMetadata dataResource) {
    boolean hasName = properties.get(DwcTerm.scientificName.simpleName()) != null;
    if (!hasName) addIssue(sr, ALAOccurrenceIssue.NAME_NOT_SUPPLIED);
    return hasName;
  }

  /**
   * Interprets a utils from the taxonomic properties supplied from the various source records.
   *
   * @param dataResource The associated data resource for the record, for defaults and hints
   * @param speciesStore The sensitive species lookup
   * @param conservationService The sensitive species service
   * @param properties The properties that have values
   * @param sr The sensitivity record
   */
  public static void sensitiveDataInterpreter(
      final ALACollectoryMetadata dataResource,
      final KeyValueStore<SpeciesCheck, Boolean> speciesStore,
      final ConservationApi conservationService,
      Map<String, String> properties,
      ALASensitivityRecord sr) {

    String scientificName = properties.get(DwcTerm.scientificName.simpleName());
    String taxonId = properties.get(DwcTerm.taxonConceptID.simpleName());
    SpeciesCheck speciesCheck =
        SpeciesCheck.builder().scientificName(scientificName).taxonId(taxonId).build();
    sr.setSensitive(speciesStore.get(speciesCheck));
    if (sr.getSensitive() == null || !sr.getSensitive()) return;
    SensitivityQuery query =
        SensitivityQuery.builder()
            .scientificName(scientificName)
            .taxonId(taxonId)
            .properties(properties)
            .build();
    SensitivityReport report = conservationService.process(query);
    sr.setSensitive(report.isSensitive());
    if (!report.isValid()) addIssue(sr, ALAOccurrenceIssue.SENSITIVITY_REPORT_INVALID);
    if (!report.isLoadable()) addIssue(sr, ALAOccurrenceIssue.SENSITIVITY_REPORT_NOT_LOADABLE);
    if (report.isSensitive()) {
      Map<String, Object> result = new HashMap<>(report.getResult());
      if (result.containsKey(DATA_GENERALIZATIONS)) {
        sr.setDataGeneralizations(result.remove(DATA_GENERALIZATIONS).toString());
      }
      if (result.containsKey(GENERALISATION_TO_APPLY_IN_METRES))
        sr.setGeneralisationToApplyInMetres(
            result.remove(GENERALISATION_TO_APPLY_IN_METRES).toString());
      if (result.containsKey(GENERALISATION_IN_METRES))
        sr.setGeneralisationInMetres(result.remove(GENERALISATION_IN_METRES).toString());
      if (result.containsKey(ORIGINAL_VALUES)) {
        sr.setOriginal((Map<String, String>) result.remove(ORIGINAL_VALUES));
      }
      sr.setAltered(
          result.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      e -> e.getKey(),
                      e -> e.getValue() == null ? null : e.getValue().toString())));
    }
  }

  /**
   * Add an issue to the issues list.
   *
   * @param sr The record
   * @param issue The issue
   */
  protected static void addIssue(ALASensitivityRecord sr, InterpretationRemark issue) {
    ModelUtils.addIssue(sr, issue.getId());
  }

  protected static String getValue(ExtendedRecord record, Term term, Set<String> recordTypes) {
    String field = term.simpleName();
    for (Map.Entry<String, List<Map<String, String>>> ext : record.getExtensions().entrySet()) {
      if (recordTypes == null || recordTypes.isEmpty() || recordTypes.contains(ext.getKey())) {
        Optional<String> value =
            ext.getValue().stream()
                .filter(m -> m.containsKey(field))
                .map(m -> m.get(field))
                .findFirst();
        if (value.isPresent()) return value.get();
      }
    }
    if (recordTypes == null
        || recordTypes.isEmpty()
        || recordTypes.contains(record.getCoreRowType())) {
      return record.getCoreTerms().get(field);
    }
    return null;
  }

  /**
   * Try and get a scientific name from one of the inputs.
   *
   * @param atxr The ALA taxon record
   * @param txr The GBIF taxon record
   * @param er The verbatim record
   * @return Any found name
   */
  public static String extractScientificName(
      final ALATaxonRecord atxr, final TaxonRecord txr, final ExtendedRecord er) {
    String scientificName = null;
    if (atxr != null && (scientificName = atxr.getScientificName()) != null) return scientificName;
    if (txr != null && (scientificName = txr.getAcceptedUsage().getName()) != null)
      return scientificName;
    if (er != null
        && (scientificName = ModelUtils.extractValue(er, DwcTerm.scientificName)) != null)
      return scientificName;
    return null;
  }

  /**
   * Try and get a taxon ID from one of the inputs.
   *
   * @param atxr The ALA taxon record
   * @param txr The GBIF taxon record
   * @param er The verbatim record
   * @return Any found name
   */
  public static String extractTaxonId(
      final ALATaxonRecord atxr, final TaxonRecord txr, final ExtendedRecord er) {
    String taxonId = null;
    if (atxr != null && (taxonId = atxr.getTaxonConceptID()) != null) return taxonId;
    if (txr != null) {
      Integer key = txr.getAcceptedUsage().getKey();
      if (key != null) return key.toString();
    }
    if (er != null) {
      if ((taxonId = ModelUtils.extractValue(er, DwcTerm.taxonConceptID)) != null) return taxonId;
      if ((taxonId = ModelUtils.extractValue(er, DwcTerm.taxonID)) != null) return taxonId;
    }
    return null;
  }
}
