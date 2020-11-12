package org.gbif.pipelines.core.ws.blast.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class Blast implements Serializable {

  private static final long serialVersionUID = 5901501300895045269L;

  private String name;
  private Integer identity;
  private String appliedScientificName;
  private String matchType;
  private Integer bitScore;
  private Integer expectValue;
  private String querySequence;
  private String subjectSequence;
  private Integer qstart;
  private Integer qend;
  private Integer sstart;
  private Integer send;
  private String distanceToBestMatch;
  private Integer sequenceLength;
}
