package org.gbif.pipelines.core.ws.sequenceclassifier.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.annotation.Nullable;
import java.io.Serial;
import java.io.Serializable;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClassifyOccurrenceResponse implements Serializable {

  @Serial private static final long serialVersionUID = -4172900351620488213L;

  private String gbifID;

  @Nullable private DnaClassification classification;

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class DnaClassification implements Serializable {

    @Serial private static final long serialVersionUID = 3305846275612094471L;

    private String scientificName;
    private String taxonRank;
    private String kingdom;
    private String phylum;

    @JsonProperty("class")
    private String clazz;

    private String order;
    private String family;
    private String genus;
    private String species;

    private String remarks;
  }
}
