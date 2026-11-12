package org.gbif.pipelines.core.phylogeny;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Terms of the Phylogenetic Material Citation extension, which links an occurrence to a tip of a
 * phylogenetic tree.
 *
 * <p>See
 * https://rs.gbif.org/sandbox/extension/gbif/1.0/phylogenetic_material_citation_2026-10-07.xml
 *
 * <p>TODO replace with the dwc-api terms once they are added there.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PhylogeneticMaterialCitationTerms {

  private static final String NS = "http://rs.gbif.org/terms/1.0/";

  public static final String ROW_TYPE = NS + "PhylogeneticMaterialCitation";

  /** The identifier of the tip on the phylogenetic tree. Required. */
  public static final String PHYLOGENETIC_TREE_TIP_ID = NS + "phylogeneticTreeTipID";

  /** The filename of the phylogenetic tree. Required. */
  public static final String PHYLOGENETIC_TREE_FILE_NAME = NS + "phylogeneticTreeFileName";

  /** The name of the phylogenetic tree within the file, should there be multiple. Optional. */
  public static final String PHYLOGENETIC_TREE_NAME = NS + "phylogeneticTreeName";
}
