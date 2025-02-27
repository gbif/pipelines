package org.gbif.pipelines.backbone.impact;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(setterPrefix = "with")
public class Identification implements Serializable {

  private String scientificNameID;
  private String taxonConceptID;
  private String taxonID;
  private String kingdom;
  private String phylum;
  private String clazz;
  private String order;
  private String family;
  private String genus;
  private String scientificName;
  private String genericName;
  private String specificEpithet;
  private String infraspecificEpithet;
  private String scientificNameAuthorship;
  private String rank;
  private String verbatimRank;
  private Boolean strict;
  private Boolean verbose;
}
