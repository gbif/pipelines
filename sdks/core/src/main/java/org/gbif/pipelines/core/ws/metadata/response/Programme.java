package org.gbif.pipelines.core.ws.metadata.response;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/** GBIF programme (portfolio of projects). */
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
