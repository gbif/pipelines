package org.gbif.converters.parser.xml.model;

import org.gbif.converters.parser.xml.constants.TaxonRankEnum;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
public class Taxon {

  private TaxonRankEnum rank;
  private String name;
}
