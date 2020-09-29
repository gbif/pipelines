package au.org.ala.pipelines.interpreters;

import au.org.ala.kvs.client.ALACollectoryMetadata;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import au.org.ala.sds.api.ConservationApi;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
import au.org.ala.sds.api.SpeciesCheck;
import java.util.*;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.gbif.api.vocabulary.InterpretationRemark;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.dwc.terms.TermFactory;
import org.gbif.kvs.KeyValueStore;
import org.gbif.pipelines.io.avro.*;
import org.gbif.pipelines.parsers.utils.ModelUtils;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SensitiveDataInterpreter {
  protected static String ORIGINAL_VALUES = "originalSensitiveValues";
  protected static String DATA_GENERALIZATIONS = "dataGeneralizations";
  protected static String GENERALISATION_TO_APPLY_IN_METRES = "generalisationToApplyInMetres";
  protected static String GENERALISATION_IN_METRES = "generalisationInMetres";

  private static Set<String> TAXON_ROWS =
      new HashSet<String>(
          Arrays.asList(
              ALATaxonRecord.getClassSchema().getName(),
              OccurrenceHdfsRecord.getClassSchema().getName()));

  /** Bits to skip when generically updating the temporal record */
  private static Set<String> SKIP_TEMPORAL_UPDATE =
      Collections.singleton(DwcTerm.eventDate.simpleName());

  /**
   * Construct information from the extended record.
   *
   * <p>If there is any extension with this value, that takes precedence. Otherwise, the core record
   * is used.
   *
   * <p>If a field is already set, the current value is ignored.
   *
   * @param sensitive The list of sensitive properties
   * @param properties The field map that we are constructing
   * @param er The extended record
   */
  public static void constructFields(
      Map<String, Term> sensitive, Map<String, String> properties, ExtendedRecord er) {
    if (er == null) return;
    er.getExtensions()
        .values()
        .forEach(el -> el.forEach(ext -> constructFields(sensitive, properties, ext)));
    constructFields(sensitive, properties, er.getCoreTerms());
  }

  /**
   * Construct information from a taxon record.
   *
   * <p>There needs to be a certain amount of decoding to translate to strict DwC
   *
   * <p>If a property is already set, the current value is ignored.
   *
   * @param sensitive The list of sensitive properties
   * @param properties The field map that we are constructing
   * @param record A record
   */
  public static void constructFields(
      Map<String, Term> sensitive, Map<String, String> properties, TaxonRecord record) {

    if (record == null) return;
    String scientificName = DwcTerm.scientificName.simpleName();
    if (sensitive.containsKey(scientificName) && !properties.containsKey(scientificName)) {
      if (record.getAcceptedUsage() != null)
        properties.put(scientificName, record.getAcceptedUsage().getName());
    }
    String taxonConceptID = DwcTerm.taxonConceptID.simpleName();
    if (sensitive.containsKey(taxonConceptID) && !properties.containsKey(taxonConceptID)) {
      if (record.getAcceptedUsage() != null)
        properties.put(taxonConceptID, record.getAcceptedUsage().getKey().toString());
    }
    String taxonRank = DwcTerm.taxonRank.simpleName();
    if (sensitive.containsKey(taxonRank) && !properties.containsKey(taxonRank)) {
      if (record.getAcceptedUsage() != null)
        properties.put(taxonRank, record.getAcceptedUsage().getRank().name());
    }
    if (record.getClassification() != null) {
      for (RankedName r : record.getClassification()) {
        String rank = r.getRank().name().toLowerCase();
        if (sensitive.containsKey(rank) && !properties.containsKey(rank)) {
          properties.put(rank, r.getName());
        }
      }
    }
    constructFields(sensitive, properties, (IndexedRecord) record);
  }

  /**
   * Construct information from a temporal record.
   *
   * <p>There needs to be a certain amount of decoding to translate to strict DwC
   *
   * <p>If a property is already set, the current value is ignored.
   *
   * @param sensitive The list of sensitive properties
   * @param properties The field map that we are constructing
   * @param record A record
   */
  public static void constructFields(
      Map<String, Term> sensitive, Map<String, String> properties, TemporalRecord record) {

    if (record == null) return;
    String eventDate = DwcTerm.eventDate.simpleName();
    if (sensitive.containsKey(eventDate) && !properties.containsKey(eventDate)) {
      if (record.getEventDate() != null) properties.put(eventDate, record.getEventDate().getGte());
    }
    constructFields(sensitive, properties, (IndexedRecord) record);
  }

  /**
   * Construct information from a generic AVRO record
   *
   * <p>If a field is already set, the current value is ignored.
   *
   * @param sensitive The list of sensitive properties
   * @param properties The field map that we are constructing
   * @param record A record
   */
  public static void constructFields(
      Map<String, Term> sensitive, Map<String, String> properties, IndexedRecord record) {

    if (record == null) return;
    for (Schema.Field f : record.getSchema().getFields()) {
      String name = f.name();
      if (sensitive.containsKey(name) && !properties.containsKey(name)) {
        Object value = record.get(f.pos());
        properties.put(name, value == null ? null : value.toString());
      }
    }
  }

  protected static void constructFields(
      Map<String, Term> sensitive, Map<String, String> properties, Map<String, String> values) {
    values
        .entrySet()
        .forEach(
            e -> {
              Term term = sensitive.get(e.getKey());
              String sn = term == null ? null : term.simpleName();
              if (sn != null && !properties.containsKey(sn)) properties.put(sn, e.getValue());
            });
  }

  /**
   * Apply sensitive data changes to a generic AVRO record.
   *
   * @param sr The sensitivity recoord
   * @param record The record to apply this to
   */
  public static void applySensitivity(
      Map<String, Term> sensitive, ALASensitivityRecord sr, IndexedRecord record) {
    applySensitivity(sensitive, sr, record, Collections.emptySet());
  }

  /**
   * Apply sensitive data changes to a temporal record.
   *
   * @param sr The sensitivity record
   * @param record The record to apply this to
   */
  public static void applySensitivity(
      Map<String, Term> sensitive, ALASensitivityRecord sr, TemporalRecord record) {
    Map<String, String> altered = sr.getAltered();
    String eventDate = DwcTerm.eventDate.simpleName();
    if (altered.containsKey(eventDate)) {
      String newDate = altered.get(eventDate);
      record.setEventDate(EventDate.newBuilder().setGte(newDate).setLte(newDate).build());
    }
    applySensitivity(sensitive, sr, record, SKIP_TEMPORAL_UPDATE);
  }

  /**
   * Apply sensitive data changes to a taxon record.
   *
   * <p>If there is an update to scientific name/rank then a new accepted usage is given and other
   * elements (classification, usage, synonym, etc. are set to null)
   *
   * @param sr The sensitivity record
   * @param record The record to apply this to
   */
  public static void applySensitivity(
      Map<String, Term> sensitive, ALASensitivityRecord sr, TaxonRecord record) {
    Map<String, String> altered = sr.getAltered();
    String scientificName = DwcTerm.scientificName.simpleName();
    String taxonRank = DwcTerm.taxonRank.simpleName();
    if (altered.containsKey(scientificName) || altered.containsKey(taxonRank)) {
      Optional<RankedName> name = Optional.ofNullable(record.getAcceptedUsage());
      String newScientificName =
          altered.getOrDefault(scientificName, name.map(RankedName::getName).orElse(null));
      String newTaxonRank =
          altered.getOrDefault(
              taxonRank,
              name.map(RankedName::getRank).map(Rank::name).orElse(Rank.UNRANKED.name()));
      record.setAcceptedUsage(
          RankedName.newBuilder()
              .setName(newScientificName)
              .setRank(Rank.valueOf(newTaxonRank.toUpperCase()))
              .build());
      record.setClassification(null);
      record.setSynonym(null);
      record.setUsage(null);
    }
  }

  /**
   * Apply sensitive data changes to an AVRO record.
   *
   * @param sr The sensitivity record
   * @param record A record
   * @param ignore Any fields to ignore (because they have already been dealt with)
   */
  // TODO: This interprets the incoming value against the schema.
  // There has to be a better way
  protected static void applySensitivity(
      Map<String, Term> sensitive,
      ALASensitivityRecord sr,
      IndexedRecord record,
      Set<String> ignore) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) return;
    for (Schema.Field f : record.getSchema().getFields()) {
      String name = f.name();
      if (altered.containsKey(name) && !ignore.contains(name)) {
        String s = altered.get(name);
        Schema schema = f.schema();
        Object v = null;
        if (s != null) {
          for (Schema type : schema.getTypes()) {
            try {
              Schema.Type st = type.getType();
              switch (type.getType()) {
                case NULL:
                  break;
                case STRING:
                  v = s;
                  break;
                case INT:
                  v = Integer.parseInt(s);
                  break;
                case LONG:
                  v = Long.parseLong(s);
                  break;
                case FLOAT:
                  v = Float.parseFloat(s);
                  break;
                case DOUBLE:
                  v = Double.parseDouble(s);
                  break;
                default:
                  throw new IllegalStateException(
                      "Unable to parse value of type "
                          + st
                          + " for field "
                          + name
                          + " in schema "
                          + schema);
              }
              if (v != null) break;
            } catch (NumberFormatException ex) {
              // Silently ignore in the hope that something comes along later
            }
          }
          if (v == null)
            throw new IllegalArgumentException(
                "Unable to parse " + s + " for field " + name + " from schema " + schema);
        }
        record.put(f.pos(), v);
      }
    }
  }

  /**
   * Apply sensitive data changes to a verbatim record.
   *
   * @param sr The sensitivity record
   * @param record A record
   */
  public static void applySensitivity(
      Map<String, Term> sensitive, ALASensitivityRecord sr, ExtendedRecord record) {
    record
        .getExtensions()
        .forEach((k, el) -> el.forEach(ext -> applySensitivity(sensitive, sr, ext)));
    applySensitivity(sensitive, sr, record.getCoreTerms());
  }

  protected static void applySensitivity(
      Map<String, Term> sensitive, ALASensitivityRecord sr, Map<String, String> values) {
    sr.getAltered()
        .forEach(
            (k, v) -> {
              Term term = sensitive.get(k);
              String qn = term == null ? null : term.qualifiedName();
              if (qn != null && values.containsKey(qn)) values.put(qn, v);
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
    boolean hasName =
        properties.get(DwcTerm.scientificName.simpleName()) != null
            || properties.get(DwcTerm.scientificName.qualifiedName()) != null;
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

  /**
   * Build a map of names onto terms
   *
   * @param fields The field names
   * @return A map of bare name/URI onto terms
   */
  public static Map<String, Term> buildSensitivityMap(Collection<String> fields) {
    TermFactory termFactory = TermFactory.instance();
    Map<String, Term> sensitvityMap = new HashMap<>();

    for (String field : fields) {
      Term term = termFactory.findTerm(field);
      sensitvityMap.put(field, term);
      sensitvityMap.put(term.simpleName(), term);
      sensitvityMap.put(term.qualifiedName(), term);
    }
    return sensitvityMap;
  }
}
