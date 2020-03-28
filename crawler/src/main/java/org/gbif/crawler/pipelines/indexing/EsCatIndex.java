package org.gbif.crawler.pipelines.indexing;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EsCatIndex implements Serializable {

  private static final long serialVersionUID = 7134020816642786944L;

  @JsonProperty("docs.count")
  private long count;

  @JsonProperty("index")
  private String name;

  public long getCount() {
    return count;
  }

  public EsCatIndex setCount(long count) {
    this.count = count;
    return this;
  }

  public String getName() {
    return name;
  }

  public EsCatIndex setName(String name) {
    this.name = name;
    return this;
  }
}
