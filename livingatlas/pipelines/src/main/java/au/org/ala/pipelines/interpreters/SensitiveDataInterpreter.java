package au.org.ala.pipelines.interpreters;

import au.org.ala.pipelines.transforms.IndexFields;
import au.org.ala.pipelines.vocabulary.ALAOccurrenceIssue;
import au.org.ala.pipelines.vocabulary.Vocab;
import au.org.ala.sds.api.SensitivityQuery;
import au.org.ala.sds.api.SensitivityReport;
import au.org.ala.sds.api.SpeciesCheck;
import au.org.ala.sds.generalise.FieldAccessor;
import au.org.ala.sds.generalise.Generalisation;
import java.util.*;
import java.util.function.Function;
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
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.*;

/** Sensitive data interpretation methods. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SensitiveDataInterpreter {
  protected static final TermFactory TERM_FACTORY = TermFactory.instance();
  protected static final Term EVENT_DATE_END_TERM =
      TERM_FACTORY.findTerm(IndexFields.EVENT_DATE_END);

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
      Set<Term> sensitive, Map<String, String> properties, ExtendedRecord er) {
    if (er == null) {
      return;
    }
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
      Set<Term> sensitive, Map<String, String> properties, TaxonRecord record) {

    if (record == null) {
      return;
    }
    RankedNameWithAuthorship name = record.getAcceptedUsage();
    if (name == null) {
      return;
    }
    constructField(
        name, DwcTerm.scientificName, sensitive, properties, RankedNameWithAuthorship::getName);
    constructField(name, DwcTerm.taxonConceptID, sensitive, properties, n -> n.getKey());
    constructField(name, DwcTerm.taxonRank, sensitive, properties, n -> n.getRank());

    if (record.getClassification() != null) {
      for (RankedName r : record.getClassification()) {
        Term rank = TERM_FACTORY.findTerm(r.getRank().toLowerCase());
        constructField(name, rank, sensitive, properties, RankedNameWithAuthorship::getName);
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
      Set<Term> sensitive, Map<String, String> properties, TemporalRecord record) {
    if (record == null) {
      return;
    }
    EventDate eventDate = record.getEventDate();
    constructField(eventDate, DwcTerm.eventDate, sensitive, properties, EventDate::getGte);
    constructField(eventDate, EVENT_DATE_END_TERM, sensitive, properties, EventDate::getLte);
    constructField(record, DwcTerm.day, sensitive, properties, TemporalRecord::getDay);
    constructField(record, DwcTerm.month, sensitive, properties, TemporalRecord::getMonth);
    constructField(record, DwcTerm.year, sensitive, properties, TemporalRecord::getYear);
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
      Set<Term> sensitive, Map<String, String> properties, IndexedRecord record) {
    if (record == null) {
      return;
    }
    for (Schema.Field f : record.getSchema().getFields()) {
      Term term = TERM_FACTORY.findTerm(f.name());
      if (term != null) {
        String name = term.qualifiedName();
        if (sensitive.contains(term) && !properties.containsKey(name)) {
          Object value = record.get(f.pos());
          properties.put(name, value == null ? null : value.toString());
        }
      } else {
        throw new RuntimeException("Unrecognised field name: " + f.name());
      }
    }
  }

  protected static void constructFields(
      Set<Term> sensitive, Map<String, String> properties, Map<String, String> values) {
    values.forEach(
        (key, value) -> {
          Term term = TERM_FACTORY.findTerm(key);
          if (sensitive.contains(term)) {
            String sn = term == null ? null : term.qualifiedName();
            if (sn != null && properties.get(sn) == null) {
              properties.put(sn, value);
            }
          }
        });
  }

  /**
   * General purpose getter of values for a field.
   *
   * @param record The record to get the value from
   * @param term The term to use
   * @param fields The set of sensitive fields
   * @param properties The current set of properties
   * @param getter A function that gets the appropriate
   * @param <R> The type of the record
   * @param <V> The value expected for the properties
   */
  protected static <R, V> void constructField(
      R record,
      Term term,
      Set<Term> fields,
      Map<String, String> properties,
      Function<R, V> getter) {
    if (!fields.contains(term)) {
      return;
    }
    String name = term.qualifiedName();
    if (properties.containsKey(name)) {
      return;
    }
    V value = getter.apply(record);
    properties.put(name, value == null ? null : value.toString());
  }

  /**
   * Apply sensitive data changes to a generic AVRO record.
   *
   * @param sr The sensitivity recoord
   * @param record The record to apply this to
   */
  public static void applySensitivity(
      Set<Term> sensitive, ALASensitivityRecord sr, IndexedRecord record) {
    applySensitivity(sensitive, sr, record, Collections.emptySet());
  }

  /**
   * Apply sensitive data changes to a temporal record.
   *
   * @param sr The sensitivity record
   * @param record The record to apply this to
   */
  public static void applySensitivity(
      Set<Term> sensitive, ALASensitivityRecord sr, TemporalRecord record) {
    Map<String, String> altered = sr.getAltered();
    String eventDate = DwcTerm.eventDate.qualifiedName();
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
      Set<Term> sensitive, ALASensitivityRecord sr, TaxonRecord record) {
    Map<String, String> altered = sr.getAltered();
    String scientificName = DwcTerm.scientificName.simpleName();
    String taxonRank = DwcTerm.taxonRank.simpleName();
    if (altered.containsKey(scientificName) || altered.containsKey(taxonRank)) {
      Optional<RankedNameWithAuthorship> name = Optional.ofNullable(record.getAcceptedUsage());
      String newScientificName =
          altered.getOrDefault(
              scientificName, name.map(RankedNameWithAuthorship::getName).orElse(null));
      String newTaxonRank =
          altered.getOrDefault(
              taxonRank, name.map(RankedNameWithAuthorship::getRank).orElse(Rank.UNRANKED.name()));
      record.setAcceptedUsage(
          RankedNameWithAuthorship.newBuilder()
              .setName(newScientificName)
              .setRank(Rank.valueOf(newTaxonRank.toUpperCase()).toString())
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
      Set<Term> sensitive, ALASensitivityRecord sr, IndexedRecord record, Set<Term> ignore) {
    Map<String, String> altered = sr.getAltered();

    if (altered == null || altered.isEmpty()) {
      return;
    }
    for (Schema.Field f : record.getSchema().getFields()) {
      Term term = TERM_FACTORY.findTerm(f.name());
      if (altered.containsKey(term.qualifiedName()) && !ignore.contains(term)) {
        String s = altered.get(term.qualifiedName());
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
                          + term
                          + " in schema "
                          + schema);
              }
              if (v != null) {
                break;
              }
            } catch (NumberFormatException ex) {
              // Silently ignore in the hope that something comes along later
            }
          }
          if (v == null) {
            throw new IllegalArgumentException(
                "Unable to parse " + s + " for field " + term + " from schema " + schema);
          }
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
      Set<Term> sensitive, ALASensitivityRecord sr, ExtendedRecord record) {
    record
        .getExtensions()
        .forEach((k, el) -> el.forEach(ext -> applySensitivity(sensitive, sr, ext, false)));
    applySensitivity(sensitive, sr, record.getCoreTerms(), true);
  }

  protected static void applySensitivity(
      Set<Term> sensitive,
      ALASensitivityRecord sr,
      Map<String, String> values,
      boolean removeNulls) {
    sr.getAltered()
        .forEach(
            (k, v) -> {
              Term term = TERM_FACTORY.findTerm(k);
              if (sensitive.contains(term)) {
                String qn = term.qualifiedName();
                if (values.containsKey(qn)) {
                  if (v != null || !removeNulls) {
                    values.put(qn, v);
                  } else {
                    values.remove(qn);
                  }
                }
              }
            });
  }

  /**
   * Perform quality checks on the incoming record.
   *
   * @param properties The potentially sensitive occurrence properties
   * @param sr The sensitivity record
   * @return True if we have enough to be going on with
   */
  public static boolean sourceQualityChecks(
      final Map<String, String> properties, final ALASensitivityRecord sr) {
    boolean hasName =
        properties.get(DwcTerm.scientificName.simpleName()) != null
            || properties.get(DwcTerm.scientificName.qualifiedName()) != null;
    if (!hasName) {
      addIssue(sr, ALAOccurrenceIssue.NAME_NOT_SUPPLIED);
    }
    return hasName;
  }

  /**
   * Interprets a utils from the taxonomic properties supplied from the various source records.
   *
   * @param speciesStore The sensitive species lookup
   * @param reportStore The sensitive data report
   * @param generalisations The generalisations to apply
   * @param properties The properties that have values
   * @param existingGeneralisations Any pre-existing generalisations
   * @param sensitivityVocab The vocabulary to use when deriving sensitivity terms
   * @param sr The sensitivity record
   */
  public static void sensitiveDataInterpreter(
      final KeyValueStore<SpeciesCheck, Boolean> speciesStore,
      final KeyValueStore<SensitivityQuery, SensitivityReport> reportStore,
      final List<Generalisation> generalisations,
      String dataResourceUid,
      Map<String, String> properties,
      Map<String, String> existingGeneralisations,
      Vocab sensitivityVocab,
      ALASensitivityRecord sr) {

    String scientificName = properties.get(DwcTerm.scientificName.qualifiedName());
    String taxonId = properties.get(DwcTerm.taxonConceptID.qualifiedName());

    SpeciesCheck speciesCheck =
        SpeciesCheck.builder().scientificName(scientificName).taxonId(taxonId).build();
    sr.setIsSensitive(speciesStore.get(speciesCheck));

    if (sr.getIsSensitive() == null || !sr.getIsSensitive()) {
      return;
    }
    String stateProvince = properties.get(DwcTerm.stateProvince.qualifiedName());
    String country = properties.get(DwcTerm.country.qualifiedName());
    SensitivityQuery query =
        SensitivityQuery.builder()
            .scientificName(scientificName)
            .taxonId(taxonId)
            .dataResourceUid(dataResourceUid)
            .stateProvince(stateProvince)
            .country(country)
            .build();
    SensitivityReport report = reportStore.get(query);
    sr.setIsSensitive(report.isSensitive());
    if (!report.isValid()) {
      addIssue(sr, ALAOccurrenceIssue.SENSITIVITY_REPORT_INVALID);
    }
    if (!report.isLoadable()) {
      addIssue(sr, ALAOccurrenceIssue.SENSITIVITY_REPORT_NOT_LOADABLE);
    }
    if (report.isSensitive()) {
      Map<String, Object> original = new HashMap<>();
      Map<String, Object> result = new HashMap<>();
      for (Generalisation generalisation : generalisations)
        generalisation.process(properties, original, result, report);
      sr.setDataGeneralizations(
          DATA_GENERALIZATIONS.get(result).getValue().map(Object::toString).orElse(null));
      sr.setInformationWithheld(
          INFORMATION_WITHHELD.get(result).getValue().map(Object::toString).orElse(null));
      sr.setGeneralisationToApplyInMetres(
          GENERALISATION_TO_APPLY_IN_METRES
              .get(result)
              .getValue()
              .map(Object::toString)
              .orElse(null));
      sr.setGeneralisationInMetres(
          GENERALISATION_IN_METRES.get(result).getValue().map(Object::toString).orElse(null));
      sr.setOriginal(toStringMap(original));
      sr.setAltered(toStringMap(result));
      // We already have notes about generalisations
      boolean alreadyGeneralised = !existingGeneralisations.isEmpty();
      // The message contains a note about already generalising things
      if (sr.getDataGeneralizations() != null) {
        alreadyGeneralised =
            alreadyGeneralised
                || sr.getDataGeneralizations().contains("already generalised")
                || sr.getDataGeneralizations().contains("already generalized");
      }
      // The lat/long hasn't changed
      Optional<Double> originalLat =
          DECIMAL_LATITUDE.get(original).getValue().map(SensitiveDataInterpreter::parseDouble);
      Optional<Double> generalisedLat =
          DECIMAL_LATITUDE.get(result).getValue().map(SensitiveDataInterpreter::parseDouble);
      Optional<Double> originalLong =
          DECIMAL_LONGITUDE.get(original).getValue().map(SensitiveDataInterpreter::parseDouble);
      Optional<Double> generalisedLong =
          DECIMAL_LONGITUDE.get(result).getValue().map(SensitiveDataInterpreter::parseDouble);
      if (originalLat.isPresent()
          && generalisedLat.isPresent()
          && originalLong.isPresent()
          && generalisedLong.isPresent()) {
        if (Math.abs(originalLat.get() - generalisedLat.get()) < UNALTERED
            && Math.abs(originalLong.get() - generalisedLong.get()) < UNALTERED)
          alreadyGeneralised = true;
      }
      String sensitivity = alreadyGeneralised ? "alreadyGeneralised" : "generalised";
      if (sensitivityVocab != null) {
        sensitivity = sensitivityVocab.matchTerm(sensitivity).orElse(sensitivity);
      }
      sr.setSensitive(sensitivity);
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
    String scientificName;
    if (atxr != null && (scientificName = atxr.getScientificName()) != null) {
      return scientificName;
    }
    if (txr != null && (scientificName = txr.getAcceptedUsage().getName()) != null) {
      return scientificName;
    }
    if (er != null
        && (scientificName = ModelUtils.extractValue(er, DwcTerm.scientificName)) != null) {
      return scientificName;
    }
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
    String taxonId;
    if (atxr != null && (taxonId = atxr.getTaxonConceptID()) != null) {
      return taxonId;
    }
    if (txr != null) {
      String key = txr.getAcceptedUsage().getKey();
      if (key != null) {
        return key;
      }
    }
    if (er != null) {
      if ((taxonId = ModelUtils.extractValue(er, DwcTerm.taxonConceptID)) != null) {
        return taxonId;
      }
      if ((taxonId = ModelUtils.extractValue(er, DwcTerm.taxonID)) != null) {
        return taxonId;
      }
    }
    return null;
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

  /**
   * Try to parse a double.
   *
   * @param v The double
   * @return A resulting double or empty value if null or unparsable.
   */
  protected static Double parseDouble(Object v) {
    if (v == null) return null;
    if (v instanceof Number) return ((Number) v).doubleValue();
    String vs = v.toString();
    if (vs.isEmpty()) return null;
    try {
      return Double.parseDouble(vs);
    } catch (NumberFormatException ex) {
      return null;
    }
  }
}
