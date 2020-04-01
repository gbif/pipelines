package org.gbif.pipelines.crawler.indexing;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EsCatIndex implements Serializable {

  private static final long serialVersionUID = 7134020816642786944L;

  @JsonProperty("docs.count")
  private long count;

  @JsonProperty("index")
  private String name;
}
