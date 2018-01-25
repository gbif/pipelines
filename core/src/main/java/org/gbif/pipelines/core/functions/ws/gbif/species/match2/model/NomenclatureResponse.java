package org.gbif.pipelines.core.functions.ws.gbif.species.match2.model;

import java.io.Serializable;

public class NomenclatureResponse implements Serializable {

  private static final long serialVersionUID = 829422886059883606L;

  private String source;
  private String id;

  public String getSource() {
    return source;
  }

  public void setSource(String source) {
    this.source = source;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
}
