package org.gbif.converters.parser.xml.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.gbif.converters.parser.xml.constants.TaxonRankEnum;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Taxon {

  private TaxonRankEnum rank;
  private String name;
}
