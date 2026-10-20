package org.gbif.pipelines.core.ws.sequenceclassifier.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class ClassifyOccurrenceRequest {

  private String gbifID;
  private Double decimalLatitude;
  private Double decimalLongitude;
  private List<NucleotideSequence> nucleotideSequence;

  @Getter
  @Setter
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class NucleotideSequence {

    private String nucleotideSequenceID;
    private String sequence;
    private Boolean invalid;
  }
}
