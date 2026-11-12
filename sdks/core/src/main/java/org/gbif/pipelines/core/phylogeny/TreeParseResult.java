package org.gbif.pipelines.core.phylogeny;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/** Result of reading a phylogenetic tree file with {@link PhylogeneticTreeReader}. */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TreeParseResult implements Serializable {

  @Serial private static final long serialVersionUID = 2187644307162270853L;

  private String fileName;

  /** Format the file was read as, upper case, e.g. NEXUS. Null if it isn't supported. */
  private String format;

  private Status status;

  /** Names of the trees in the file, in file order. Empty when {@link Status#INVALID}. */
  @Builder.Default private List<String> treeNames = new ArrayList<>();

  /** Tips of all trees in the file. Empty when {@link Status#INVALID}. */
  @Builder.Default private List<TreeTip> tips = new ArrayList<>();

  /** Names of the repairs that were applied, e.g. {@code EXPONENT_UNDERSCORE} */
  @Builder.Default private List<String> repairsApplied = new ArrayList<>();

  /** Parser errors, from the first attempt and, if any, from the retry after the repairs */
  @Builder.Default private List<String> errors = new ArrayList<>();

  /** Problems found in trees that were parsed, e.g. tip labels repeated within a tree */
  @Builder.Default private List<String> warnings = new ArrayList<>();

  public enum Status {
    /** Parsed on the first attempt */
    VALID,
    /** Invalid, but parsed after {@link PhylogeneticTreeReader#repair repairing} it */
    REPAIRED,
    /** Couldn't be parsed, even after the repairs */
    INVALID
  }

  /** A tip (external node) of a phylogenetic tree. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class TreeTip implements Serializable {

    @Serial private static final long serialVersionUID = -3168712345982317601L;

    /** File the tree comes from, as listed by the registry */
    private String fileName;

    /** Name of the tree within the file, see {@link PhylogeneticTreeReader} for unnamed trees */
    private String treeName;

    /** Label of the tip, matched against the extension's phylogeneticTreeTipID */
    private String tipId;
  }
}
