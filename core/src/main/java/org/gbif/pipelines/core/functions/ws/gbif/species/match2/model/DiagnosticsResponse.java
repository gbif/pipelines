package org.gbif.pipelines.core.functions.ws.gbif.species.match2.model;

import java.io.Serializable;
import java.util.List;

import com.google.gson.annotations.SerializedName;

/**
 * Models a diagnostics response.
 */
public class DiagnosticsResponse implements Serializable {

  private static final long serialVersionUID = -1036059768581926145L;

  private String matchType;
  private Integer confidence;
  @SerializedName("status")
  private String taxonomicStatus;
  private List<String> lineage;
  private List<SpeciesMatch2ResponseModel> alternatives;
  private String note;

  public String getMatchType() { return matchType; }

  public void setMatchType(String matchType) { this.matchType = matchType; }

  public Integer getConfidence() {
    return confidence;
  }

  public void setConfidence(Integer confidence) {
    this.confidence = confidence;
  }

  public String getTaxonomicStatus() {
    return taxonomicStatus;
  }

  public void setTaxonomicStatus(String taxonomicStatus) {
    this.taxonomicStatus = taxonomicStatus;
  }

  public List<String> getLineage() {
    return lineage;
  }

  public void setLineage(List<String> lineage) {
    this.lineage = lineage;
  }

  public List<SpeciesMatch2ResponseModel> getAlternatives() {
    return alternatives;
  }

  public void setAlternatives(List<SpeciesMatch2ResponseModel> alternatives) {
    this.alternatives = alternatives;
  }

  public String getNote() {
    return note;
  }

  public void setNote(String note) {
    this.note = note;
  }
}
