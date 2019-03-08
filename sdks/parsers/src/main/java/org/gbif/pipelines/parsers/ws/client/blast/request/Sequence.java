package org.gbif.pipelines.parsers.ws.client.blast.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Sequence {

  private static final long serialVersionUID = 5901501396585045269L;

  private String marker;
  private String sequence;
}
