package org.gbif.pipelines.core.functions.ws.gbif.species.match2.model;

import java.io.Serializable;
import java.util.List;

/*
 * Models a response from the species match 2 WS.
 */
public class SpeciesMatch2ResponseModel implements Serializable {

  private static final long serialVersionUID = 3816517101918253772L;

  private boolean synonym;
  private RankedNameResponse usage;
  private RankedNameResponse acceptedUsage;
  private NomenclatureResponse nomenclature;
  private List<RankedNameResponse> classification;
  private DiagnosticsResponse diagnostics;

  public boolean isSynonym() {
    return synonym;
  }

  public void setSynonym(boolean synonym) {
    this.synonym = synonym;
  }

  public RankedNameResponse getUsage() {
    return usage;
  }

  public void setUsage(RankedNameResponse usage) {
    this.usage = usage;
  }

  public RankedNameResponse getAcceptedUsage() {
    return acceptedUsage;
  }

  public void setAcceptedUsage(RankedNameResponse acceptedUsage) {
    this.acceptedUsage = acceptedUsage;
  }

  public NomenclatureResponse getNomenclature() {
    return nomenclature;
  }

  public void setNomenclature(NomenclatureResponse nomenclature) {
    this.nomenclature = nomenclature;
  }

  public List<RankedNameResponse> getClassification() {
    return classification;
  }

  public void setClassification(List<RankedNameResponse> classification) {
    this.classification = classification;
  }

  public DiagnosticsResponse getDiagnostics() {
    return diagnostics;
  }

  public void setDiagnostics(DiagnosticsResponse diagnostics) {
    this.diagnostics = diagnostics;
  }
}
