package org.gbif.pipelines.clustering;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.scala.DefaultScalaModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceFeatures;
import scala.collection.JavaConverters;
import scala.collection.Seq;

/**
 * A wrapper around a Spark row giving exposing the terms necessary for clustering. This allows
 * terms to have a prefix, allowing a Row containing multiple records to be used (e.g. as a result
 * of a SQL select a.*,b.* from a join b).
 */
public class RowOccurrenceFeatures implements OccurrenceFeatures {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  static {
    // required for e.g. correct empty array serializations
    OBJECT_MAPPER.registerModule(new DefaultScalaModule());
  }

  private final Row row;
  private final String prefix; // if field names carry prefixes e.g. t1_scientificName has t1_

  // this hack is only used to serialize fields containing JSON correctly
  private final Set<String> jsonFields;

  /**
   * @param row The underlying DataSet row to expose
   * @param prefix The prefix to strip from field names
   * @param jsonFields Fields in Row already stored as JSON strings supplied with prefixes.
   */
  public RowOccurrenceFeatures(Row row, String prefix, String... jsonFields) {
    this.row = row;
    this.prefix = prefix == null ? "" : prefix;
    this.jsonFields =
        jsonFields == null ? new HashSet<>() : new HashSet<>(Arrays.asList(jsonFields));
  }

  public RowOccurrenceFeatures(Row row, String prefix) {
    this(row, prefix, (String) null);
  }

  public RowOccurrenceFeatures(Row row) {
    this(row, "");
  }

  // This generics solution is an attempt to mirror the Scala bridge
  @SuppressWarnings("TypeParameterUnusedInFormals")
  <T> T get(String field) {
    try {
      int fieldIndex = row.fieldIndex(prefix + field);
      return row.isNullAt(fieldIndex) ? null : row.getAs(fieldIndex);
    } catch (IllegalArgumentException e) {
      // more friendly error
      throw new IllegalArgumentException("Field not found in row schema: " + prefix + field);
    }
  }

  Long getLong(String field) {
    return get(field);
  }

  String getString(String field) {
    return get(field);
  }

  List<String> getStrings(String... field) {
    List<String> vals = new ArrayList<>(field.length);
    Arrays.stream(field).forEach(s -> vals.add(getString(s)));
    return vals;
  }

  private static <T> T assertNotNull(T value, String message) {
    if (value == null) {
      throw new IllegalArgumentException(message);
    } else {
      return value;
    }
  }

  /**
   * @return JSON representing all fields that match the prefix, but with the prefix removed (e.g.
   *     t1_day becomes day)
   */
  public String asJson() throws IOException {
    // use the in built schema to extract all fields matching the prefix
    String[] fieldNames = row.schema().fieldNames();
    List<String> filteredFieldNames =
        Arrays.stream(fieldNames)
            .filter(s -> prefix == null || prefix.length() == 0 || s.startsWith(prefix))
            .collect(Collectors.toList());

    Map<String, Object> test = new HashMap<>();
    for (String field : filteredFieldNames) {
      Object o = row.isNullAt(row.fieldIndex(field)) ? null : row.getAs(field);

      if (jsonFields.contains(field)) {
        // Hack: parse the JSON so it will not be String encoded
        JsonNode media = OBJECT_MAPPER.readTree(String.valueOf(o));
        test.put(field.replaceFirst(prefix, ""), media);
      } else {
        test.put(field.replaceFirst(prefix, ""), o);
      }
    }

    return OBJECT_MAPPER.writeValueAsString(test);
  }

  @Override
  public String getId() {
    return get("id");
  }

  @Override
  public String getDatasetKey() {
    return get("datasetKey");
  }

  @Override
  public String getSpeciesKey() {
    return get("speciesKey");
  }

  @Override
  public String getTaxonKey() {
    return get("taxonKey");
  }

  @Override
  public String getBasisOfRecord() {
    return get("basisOfRecord");
  }

  @Override
  public Double getDecimalLatitude() {
    return get("decimalLatitude");
  }

  @Override
  public Double getDecimalLongitude() {
    return get("decimalLongitude");
  }

  @Override
  public Integer getYear() {
    return get("year");
  }

  @Override
  public Integer getMonth() {
    return get("month");
  }

  @Override
  public Integer getDay() {
    return get("day");
  }

  @Override
  public String getEventDate() {
    return get("eventDate");
  }

  @Override
  public String getScientificName() {
    return get("scientificName");
  }

  @Override
  public String getCountryCode() {
    return get("countryCode");
  }

  @Override
  public List<String> getTypeStatus() {
    return listOrNull("typeStatus");
  }

  @Override
  public String getOccurrenceID() {
    return get("occurrenceID");
  }

  @Override
  public List<String> getRecordedBy() {
    return listOrNull("recordedBy");
  }

  @Override
  public String getFieldNumber() {
    return get("fieldNumber");
  }

  @Override
  public String getRecordNumber() {
    return get("recordNumber");
  }

  @Override
  public String getCatalogNumber() {
    return get("catalogNumber");
  }

  @Override
  public List<String> getOtherCatalogNumbers() {
    return listOrNull("otherCatalogNumbers");
  }

  @Override
  public String getInstitutionCode() {
    return get("institutionCode");
  }

  @Override
  public String getCollectionCode() {
    return get("collectionCode");
  }

  List<String> listOrNull(String field) {
    Object o = get(field);
    // what follows exists only to simply testing (List) and Spark using Hive (Seq) integrations
    if (o == null) return null;
    else if (o instanceof Seq) {
      return JavaConverters.seqAsJavaListConverter((Seq<String>) o).asJava();
    } else if (o instanceof List) {
      return (List<String>) o;
    } else {
      throw new IllegalArgumentException("Expected a Seq or List for " + field);
    }
  }
}
