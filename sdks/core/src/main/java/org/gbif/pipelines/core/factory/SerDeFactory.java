package org.gbif.pipelines.core.factory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import java.util.List;
import lombok.experimental.UtilityClass;
import org.gbif.pipelines.io.avro.json.GbifClassification;
import org.gbif.pipelines.io.avro.json.OccurrenceJsonRecord;
import org.gbif.pipelines.io.avro.json.VerbatimRecord;

@UtilityClass
public class SerDeFactory {

  private static final ObjectMapper AVRO_MAPPER =
      new ObjectMapper()
          .addMixIn(GbifClassification.class, GbifClassificationMixin.class)
          .registerModule(new AvroModule());

  private static final ObjectMapper AVRO_MAPPER_NON_NULLS =
      new ObjectMapper()
          .addMixIn(GbifClassification.class, GbifClassificationMixin.class)
          .registerModule(new AvroModule())
          .setSerializationInclusion(Include.NON_EMPTY);

  private static final ObjectMapper AVRO_EVENTS_MAPPER_NON_NULLS =
      new ObjectMapper()
          .addMixIn(GbifClassification.class, GbifClassificationMixin.class)
          .addMixIn(OccurrenceJsonRecord.class, OccurrenceJsonRecordEventMixin.class)
          .registerModule(new AvroModule())
          .setSerializationInclusion(Include.NON_EMPTY);

  public static ObjectMapper avroMapperWithNulls() {
    return AVRO_MAPPER;
  }

  public static ObjectMapper avroMapperNonNulls() {
    return AVRO_MAPPER_NON_NULLS;
  }

  public static ObjectMapper avroEventsMapperNonNulls() {
    return AVRO_EVENTS_MAPPER_NON_NULLS;
  }

  public interface GbifClassificationMixin {

    @JsonProperty("class")
    String getClass$();
  }

  public interface OccurrenceJsonRecordEventMixin {

    @JsonIgnore
    List<String> getAll();

    @JsonIgnore
    String getLastCrawled();

    @JsonIgnore
    String getCreated();

    @JsonIgnore
    String getDatasetKey();

    @JsonIgnore
    Integer getCrawlId();

    @JsonIgnore
    String getDatasetTitle();

    @JsonIgnore
    String getInstallationKey();

    @JsonIgnore
    String getHostingOrganizationKey();

    @JsonIgnore
    String getEndorsingNodeKey();

    @JsonIgnore
    String getPublisherTitle();

    @JsonIgnore
    String getLicense();

    @JsonIgnore
    String getProtocol();

    @JsonIgnore
    String getDatasetPublishingCountry();

    @JsonIgnore
    String getPublishingOrganizationKey();

    @JsonIgnore
    List<String> getNetworkKeys();

    @JsonIgnore
    String getProjectId();

    @JsonIgnore
    String getProgrammeAcronym();

    @JsonIgnore
    VerbatimRecord getVerbatim();
  }
}
