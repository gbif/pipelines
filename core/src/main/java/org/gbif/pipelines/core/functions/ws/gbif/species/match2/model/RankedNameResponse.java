package org.gbif.pipelines.core.functions.ws.gbif.species.match2.model;

import java.io.Serializable;

/**
 * Models a RankedName response.
 */
public class RankedNameResponse implements Serializable {

  private static final long serialVersionUID = 2139732650633250955L;

  private int key;
  private String name;
  private String rank;

  public int getKey() {
    return key;
  }

  public void setKey(int key) {
    this.key = key;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getRank() {
    return rank;
  }

  public void setRank(String rank) {
    this.rank = rank;
  }
}
