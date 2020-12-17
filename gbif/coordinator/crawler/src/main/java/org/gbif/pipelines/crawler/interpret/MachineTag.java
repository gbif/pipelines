package org.gbif.pipelines.crawler.interpret;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@JsonIgnoreProperties(ignoreUnknown = true)
public class MachineTag implements Serializable {

  private static final long serialVersionUID = 7134020816642786945L;

  @JsonProperty("name")
  private String name;

  @JsonProperty("value")
  private String value;
}
