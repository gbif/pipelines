package org.gbif.pipelines.core.phylogeny;

import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.forester.io.parsers.PhylogenyParser;
import org.forester.io.parsers.nexus.NexusPhylogeniesParser;
import org.forester.io.parsers.nhx.NHXParser;
import org.forester.phylogeny.Phylogeny;
import org.forester.phylogeny.PhylogenyNode;
import org.forester.phylogeny.iterators.PhylogenyNodeIterator;
import org.gbif.pipelines.core.phylogeny.TreeParseResult.Status;
import org.gbif.pipelines.core.phylogeny.TreeParseResult.TreeTip;

/**
 * Reads the tips of the trees in a tree file, using BioJava's build of forester. The supported
 * formats are the ones in {@link #createParser}.
 *
 * <p>If the file can't be parsed, it is {@link #repair repaired} and parsed again. Files that still
 * fail, or whose format isn't supported, are returned as {@link Status#INVALID} with their errors;
 * this class never throws for malformed content.
 *
 * <p>Unnamed trees (Newick files, or Nexus trees without a name) are named {@code tree{n}} by their
 * 1-based position in the file.
 */
@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class PhylogeneticTreeReader {

  static final String EXPONENT_UNDERSCORE = "EXPONENT_UNDERSCORE";

  // a branch length whose exponent sign was replaced by '_', e.g. ":1.884795486e_05"
  private static final Pattern EXPONENT_UNDERSCORE_PATTERN =
      Pattern.compile("(:\\s*-?\\d+(?:\\.\\d+)?)[eE]_(\\d+)");

  /**
   * @param fileName name of the file, as listed by the registry
   * @param format name of the format as reported by the registry, e.g. NEXUS
   * @param content raw content of the file
   */
  public static TreeParseResult read(String fileName, String format, String content) {
    TreeParseResult result = TreeParseResult.builder().fileName(fileName).build();

    String treeFormat = format == null ? "" : format.trim().toUpperCase(Locale.ROOT);
    PhylogenyParser parser = createParser(treeFormat);
    if (parser == null) {
      result.setStatus(Status.INVALID);
      result.getErrors().add("Unsupported format: " + format);
      return result;
    }

    if (content == null || content.trim().isEmpty()) {
      result.setStatus(Status.INVALID);
      result.getErrors().add("Empty file");
      return result;
    }
    result.setFormat(treeFormat);

    try {
      fillTrees(result, parse(content, parser));
      result.setStatus(Status.VALID);
      return result;
    } catch (Exception e) {
      result.getErrors().add(errorMessage(e));
    }

    List<String> repairsApplied = new ArrayList<>();
    String repaired = repair(content, repairsApplied);
    if (repairsApplied.isEmpty()) {
      result.setStatus(Status.INVALID);
      return result;
    }

    try {
      // a new parser, they keep state from the previous attempt
      fillTrees(result, parse(repaired, createParser(treeFormat)));
      result.setStatus(Status.REPAIRED);
      result.setRepairsApplied(repairsApplied);
      log.info("Phylogeny file {} repaired with {}", fileName, repairsApplied);
    } catch (Exception e) {
      result.getErrors().add(errorMessage(e));
      result.setStatus(Status.INVALID);
    }
    return result;
  }

  /**
   * Fixes known formatting problems in published tree files and adds to {@code applied} the names
   * of the fixes that changed the content. Fixes only touch branch lengths (the numbers after
   * {@code :}), never tip labels.
   */
  @VisibleForTesting
  static String repair(String content, List<String> applied) {
    // the exponent sign was replaced by '_', e.g. by a script renaming tip labels:
    // 1.8e_05 -> 1.8e-05
    String repaired = EXPONENT_UNDERSCORE_PATTERN.matcher(content).replaceAll("$1e-$2");
    if (!repaired.equals(content)) {
      applied.add(EXPONENT_UNDERSCORE);
    }
    return repaired;
  }

  /**
   * A new parser for the format, named as the registry reports it, or null if the format isn't
   * supported. To support a new format, add a case. Parsers must keep tip labels as they are (no
   * underscore replacement, no taxonomy extraction), since labels are matched against the
   * extension's phylogeneticTreeTipID.
   */
  private static PhylogenyParser createParser(String format) {
    switch (format) {
      case "NEXUS":
        NexusPhylogeniesParser nexus = new NexusPhylogeniesParser();
        nexus.setTaxonomyExtraction(NHXParser.TAXONOMY_EXTRACTION.NO);
        nexus.setReplaceUnderscores(false);
        return nexus;
      case "NEWICK":
        NHXParser newick = new NHXParser();
        newick.setTaxonomyExtraction(NHXParser.TAXONOMY_EXTRACTION.NO);
        newick.setReplaceUnderscores(false);
        return newick;
      default:
        return null;
    }
  }

  private static Phylogeny[] parse(String content, PhylogenyParser parser) throws Exception {
    // a String source would be taken as a file path
    parser.setSource(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    Phylogeny[] trees = parser.parse();
    if (trees == null || trees.length == 0) {
      throw new IllegalArgumentException("No trees found");
    }
    return trees;
  }

  private static void fillTrees(TreeParseResult result, Phylogeny[] trees) {
    List<String> treeNames = new ArrayList<>();
    List<TreeTip> tips = new ArrayList<>();
    List<String> warnings = new ArrayList<>();
    Set<String> usedNames = new HashSet<>();

    for (int i = 0; i < trees.length; i++) {
      String treeName = uniqueTreeName(trees[i].getName(), i + 1, usedNames);
      treeNames.add(treeName);

      // tips are matched by label only, so a label repeated within a tree is kept once (tips with
      // the same label can't be told apart) and reported, as it's usually a mistake in the tree
      Map<String, Integer> tipIdCounts = new LinkedHashMap<>();
      for (PhylogenyNodeIterator it = trees[i].iteratorExternalForward(); it.hasNext(); ) {
        PhylogenyNode node = it.next();
        String name = node.getName();
        // a tip without a label can't be linked to an occurrence
        if (name != null && !name.trim().isEmpty()) {
          tipIdCounts.merge(name.trim(), 1, Integer::sum);
        }
      }

      tipIdCounts.forEach(
          (tipId, count) -> {
            tips.add(
                TreeTip.builder()
                    .fileName(result.getFileName())
                    .treeName(treeName)
                    .tipId(tipId)
                    .build());
            if (count > 1) {
              warnings.add(
                  String.format(
                      "Tip label '%s' appears %d times in tree '%s'", tipId, count, treeName));
            }
          });
    }

    result.setTreeNames(treeNames);
    result.setTips(tips);
    result.setWarnings(warnings);
  }

  /**
   * Nexus marks the default tree with a leading '*' (e.g. "* UNTITLED"), which is not part of the
   * name. Blank names become tree{position}, and repeated names get the position appended.
   */
  static String uniqueTreeName(String rawName, int position, Set<String> usedNames) {
    String name = rawName == null ? "" : rawName.trim();
    if (name.startsWith("*")) {
      name = name.substring(1).trim();
    }
    if (name.isEmpty()) {
      name = "tree" + position;
    }
    // the suffixed name can be taken too, e.g. trees "a", "a_3", "a"
    String unique = name;
    while (!usedNames.add(unique)) {
      unique = unique + "_" + position;
    }
    return unique;
  }

  private static String errorMessage(Exception e) {
    return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
  }
}
