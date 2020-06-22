package org.gbif.pipelines.parsers.ws.client.metadata.response;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** GBIF programme (portfolio of projects).*/
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class Programme implements Serializable {

  private String id;
  private String title;
  private String acronym;

}
