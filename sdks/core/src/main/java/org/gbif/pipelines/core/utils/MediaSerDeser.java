package org.gbif.pipelines.core.utils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.type.CollectionType;
import java.io.IOException;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificData;
import org.gbif.pipelines.core.pojo.HumboldtJsonView;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.VocabularyConcept;

/** Utility class to serialize and deserialize MediaObject instances from/to JSON. */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MediaSerDeser {

  abstract static class IgnoreSchemaProperty {

    // You have to use the correct package for JsonIgnore,
    // fasterxml or codehaus
    @JsonIgnore
    abstract void getSchema();

    @JsonIgnore
    abstract void getSpecificData();

    @JsonIgnore private static SpecificData MODEL$;
  }

  public abstract static class IgnoreVocabs extends IgnoreSchemaProperty {
    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.VocabularyConcept> getTargetLifeStageScope();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.VocabularyConcept>
        getExcludedLifeStageScope();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.VocabularyConcept>
        getTargetDegreeOfEstablishmentScope();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.VocabularyConcept>
        getExcludedDegreeOfEstablishmentScope();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.TaxonHumboldtRecord>
        getTargetTaxonomicScope();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.TaxonHumboldtRecord>
        getExcludedTaxonomicScope();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.TaxonHumboldtRecord> getAbsentTaxa();

    @JsonIgnore
    abstract java.util.List<org.gbif.pipelines.io.avro.TaxonHumboldtRecord> getNonTargetTaxa();
  }

  private static final String SER_ERROR_MSG = "Unable to serialize %s objects to JSON";
  private static final String DESER_ERROR_MSG = "Unable to deserialize String into media objects";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    // Don't change this section, methods used here guarantee backwards compatibility with Jackson
    // 1.8.8
    MAPPER.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
    MAPPER.configure(SerializationFeature.INDENT_OUTPUT, true);
    MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
    MAPPER.addMixIn(Multimedia.class, IgnoreSchemaProperty.class);
    MAPPER.addMixIn(Humboldt.class, IgnoreVocabs.class);
    MAPPER.addMixIn(VocabularyConcept.class, IgnoreSchemaProperty.class);
    MAPPER.addMixIn(RankedName.class, IgnoreSchemaProperty.class);
    MAPPER.addMixIn(IssueRecord.class, IgnoreSchemaProperty.class);
  }

  private static final CollectionType LIST_MEDIA_TYPE =
      MAPPER.getTypeFactory().constructCollectionType(List.class, Multimedia.class);

  /** Converts the list of media objects into a JSON string. */
  @SneakyThrows
  public static String multimediaToJson(List<Multimedia> media) {
    return media != null && !media.isEmpty() ? objectToJson(media) : null;
  }

  /** Converts the list of humboldt objects into a JSON string. */
  @SneakyThrows
  public static String humboldtToJson(List<HumboldtJsonView> humboldtJsonViews) {
    return humboldtJsonViews != null && !humboldtJsonViews.isEmpty()
        ? objectToJson(humboldtJsonViews)
        : null;
  }

  @SneakyThrows
  private static <T> String objectToJson(T obj) {
    try {
      if (obj != null) {
        return MAPPER.writeValueAsString(obj);
      }
    } catch (IOException ex) {
      log.error(String.format(SER_ERROR_MSG, obj.getClass().getSimpleName()), ex);
      throw ex;
    }
    return null;
  }

  /** Converts a Json string into a list of media objects. */
  @SneakyThrows
  public static List<Multimedia> multimediaFromJson(String mediaJson) {
    try {
      return MAPPER.readValue(mediaJson, LIST_MEDIA_TYPE);
    } catch (IOException ex) {
      log.error(DESER_ERROR_MSG, ex);
      throw ex;
    }
  }
}
