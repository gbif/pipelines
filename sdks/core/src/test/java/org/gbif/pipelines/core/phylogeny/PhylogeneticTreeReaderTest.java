package org.gbif.pipelines.core.phylogeny;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gbif.pipelines.core.phylogeny.TreeParseResult.Status;
import org.gbif.pipelines.core.phylogeny.TreeParseResult.TreeTip;
import org.junit.Assert;
import org.junit.Test;

public class PhylogeneticTreeReaderTest {

  // same shape as the sample file in the test registry: written by the R package ape (TRANSLATE
  // block, '*' default tree, NaN lengths), then '-' replaced by '_' in some exponents (e_05)
  private static final String SAMPLE_NEXUS =
      "#NEXUS\n"
          + "[R_package APE, Tue Feb 20 12:16:37 2024]\n"
          + "BEGIN TAXA;\n"
          + "\tDIMENSIONS NTAX = 3;\n"
          + "\tTAXLABELS\n"
          + "\t\tFabales_Fabaceae_Paramacrolobium_coeruleum\n"
          + "\t\tLamiales_Lamiaceae_Lavandula_angustifolia\n"
          + "\t\tEphedrales_Ephedraceae_Ephedra_sinica\n"
          + "\t;\n"
          + "END;\n"
          + "BEGIN TREES;\n"
          + "\tTRANSLATE\n"
          + "\t\t1\tFabales_Fabaceae_Paramacrolobium_coeruleum,\n"
          + "\t\t2\tLamiales_Lamiaceae_Lavandula_angustifolia,\n"
          + "\t\t3\tEphedrales_Ephedraceae_Ephedra_sinica\n"
          + "\t;\n"
          + "\tTREE * UNTITLED = [&U] ((1:NaN,2:1.884795486e_05)1/74:0.9067188153,3:NaN);\n"
          + "END;\n";

  @Test
  public void newickTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read(
            "tree.nwk", "NEWICK", "((Puma_concolor:0.1,Felis_catus:0.2):0.3,Canis_lupus:0.4);");

    Assert.assertEquals(Status.VALID, result.getStatus());
    Assert.assertEquals(Collections.singletonList("tree1"), result.getTreeNames());
    Assert.assertEquals(
        set("Puma_concolor", "Felis_catus", "Canis_lupus"), tipIds(result.getTips(), "tree1"));
    Assert.assertTrue(result.getRepairsApplied().isEmpty());
    Assert.assertTrue(result.getErrors().isEmpty());
    Assert.assertTrue(result.getWarnings().isEmpty());
    result.getTips().forEach(t -> Assert.assertEquals("tree.nwk", t.getFileName()));
  }

  @Test
  public void nhxLabelsAreKeptTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read(
            "tree.nhx",
            "NEWICK",
            "(Homo_sapiens:0.1[&&NHX:S=Human:T=9606],Pan_troglodytes:0.2[&&NHX:S=Chimpanzee:T=9598]);");

    Assert.assertEquals(Status.VALID, result.getStatus());
    Assert.assertEquals(set("Homo_sapiens", "Pan_troglodytes"), tipIds(result.getTips(), "tree1"));
  }

  @Test
  public void nexusWithTranslateAndSeveralTreesTest() {
    String content =
        "#NEXUS\n"
            + "BEGIN TREES;\n"
            + "  TRANSLATE\n"
            + "    1 Puma_concolor,\n"
            + "    2 Felis_catus,\n"
            + "    3 Canis_lupus;\n"
            + "  TREE tree_a = ((1:0.1,2:0.2):0.3,3:0.4);\n"
            + "  TREE tree_b = ((1:0.1,3:0.2):0.3,2:0.4);\n"
            + "END;\n";

    TreeParseResult result = PhylogeneticTreeReader.read("cats.nex", "NEXUS", content);

    Assert.assertEquals(Status.VALID, result.getStatus());
    Assert.assertEquals(Arrays.asList("tree_a", "tree_b"), result.getTreeNames());
    Set<String> expected = set("Puma_concolor", "Felis_catus", "Canis_lupus");
    Assert.assertEquals(expected, tipIds(result.getTips(), "tree_a"));
    Assert.assertEquals(expected, tipIds(result.getTips(), "tree_b"));
  }

  @Test
  public void exponentUnderscoreIsRepairedTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read("global_tree_renamed.nex", "NEXUS", SAMPLE_NEXUS);

    Assert.assertEquals(Status.REPAIRED, result.getStatus());
    Assert.assertEquals(
        Collections.singletonList(PhylogeneticTreeReader.EXPONENT_UNDERSCORE),
        result.getRepairsApplied());
    // the error of the first attempt is kept for the metadata
    Assert.assertEquals(1, result.getErrors().size());
    Assert.assertTrue(result.getErrors().get(0).contains("1.884795486e_05"));
    // '*' marks the default tree, it isn't part of the name
    Assert.assertEquals(Collections.singletonList("UNTITLED"), result.getTreeNames());
    Assert.assertEquals(
        set(
            "Fabales_Fabaceae_Paramacrolobium_coeruleum",
            "Lamiales_Lamiaceae_Lavandula_angustifolia",
            "Ephedrales_Ephedraceae_Ephedra_sinica"),
        tipIds(result.getTips(), "UNTITLED"));
  }

  @Test
  public void repeatedTipLabelsAreKeptOnceAndReportedTest() {
    String content =
        "#NEXUS\n"
            + "BEGIN TREES;\n"
            + "  TREE tree_a = ((Puma_concolor:0.1,Puma_concolor:0.2):0.3,(Canis_lupus:0.4,"
            + "(Canis_lupus:0.5,Canis_lupus:0.6):0.7):0.8);\n"
            + "  TREE tree_b = ((Puma_concolor:0.1,Felis_catus:0.2):0.3,Canis_lupus:0.4);\n"
            + "END;\n";

    TreeParseResult result = PhylogeneticTreeReader.read("cats.nex", "NEXUS", content);

    Assert.assertEquals(Status.VALID, result.getStatus());
    Assert.assertEquals(
        Arrays.asList("Puma_concolor", "Canis_lupus"),
        result.getTips().stream()
            .filter(t -> "tree_a".equals(t.getTreeName()))
            .map(TreeTip::getTipId)
            .collect(Collectors.toList()));
    // the same label in different trees is fine
    Assert.assertEquals(
        set("Puma_concolor", "Felis_catus", "Canis_lupus"), tipIds(result.getTips(), "tree_b"));
    Assert.assertEquals(
        Arrays.asList(
            "Tip label 'Puma_concolor' appears 2 times in tree 'tree_a'",
            "Tip label 'Canis_lupus' appears 3 times in tree 'tree_a'"),
        result.getWarnings());
  }

  @Test
  public void repairExponentUnderscoreTest() {
    List<String> applied = new ArrayList<>();

    String repaired =
        PhylogeneticTreeReader.repair("((A:1.884795486e_05,B:2E_3):0.1,C:-4.1e_02);", applied);

    Assert.assertEquals("((A:1.884795486e-05,B:2e-3):0.1,C:-4.1e-02);", repaired);
    Assert.assertEquals(Collections.singletonList("EXPONENT_UNDERSCORE"), applied);
  }

  @Test
  public void repairDoesNotTouchLabelsTest() {
    List<String> applied = new ArrayList<>();
    // looks like an exponent, but it is a tip label, not a branch length
    String content = "(Genus_1e_05:0.1,B:0.2);";

    Assert.assertEquals(content, PhylogeneticTreeReader.repair(content, applied));
    Assert.assertTrue(applied.isEmpty());
  }

  @Test
  public void nothingToRepairTest() {
    List<String> applied = new ArrayList<>();
    String content = "((A:1.8e-05,B:NaN):0.1,C:0.4);";

    Assert.assertEquals(content, PhylogeneticTreeReader.repair(content, applied));
    Assert.assertTrue(applied.isEmpty());
  }

  @Test
  public void nanBranchLengthsAreValidTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read("tree.nwk", "NEWICK", "((A:NaN,B:NaN):0.1,C:NaN);");

    Assert.assertEquals(Status.VALID, result.getStatus());
    Assert.assertEquals(set("A", "B", "C"), tipIds(result.getTips(), "tree1"));
  }

  @Test
  public void unfixableFileIsInvalidTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read("broken.nwk", "NEWICK", "((A:0.1,B:0.2):0.3,C:0.4;(((");

    Assert.assertEquals(Status.INVALID, result.getStatus());
    Assert.assertTrue(result.getTips().isEmpty());
    Assert.assertTrue(result.getTreeNames().isEmpty());
    Assert.assertTrue(result.getRepairsApplied().isEmpty());
    Assert.assertFalse(result.getErrors().isEmpty());
  }

  @Test
  public void stillInvalidAfterRepairTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read("broken.nwk", "NEWICK", "((A:1e_05,B:0.2):0.3,C:0.4;(((");

    Assert.assertEquals(Status.INVALID, result.getStatus());
    Assert.assertTrue(result.getTips().isEmpty());
    // errors of both attempts
    Assert.assertEquals(2, result.getErrors().size());
  }

  @Test
  public void emptyFileIsInvalidTest() {
    TreeParseResult result = PhylogeneticTreeReader.read("empty.nwk", "NEWICK", "  ");

    Assert.assertEquals(Status.INVALID, result.getStatus());
    Assert.assertEquals(Collections.singletonList("Empty file"), result.getErrors());
  }

  @Test
  public void formatIsRecordedTest() {
    Assert.assertEquals(
        "NEXUS", PhylogeneticTreeReader.read("a.nex", "NEXUS", SAMPLE_NEXUS).getFormat());
    // case-insensitive
    Assert.assertEquals(
        "NEWICK", PhylogeneticTreeReader.read("a.nwk", " newick ", "(A:0.1,B:0.2);").getFormat());
  }

  @Test
  public void missingFormatIsInvalidTest() {
    TreeParseResult result = PhylogeneticTreeReader.read("a.nwk", null, "(A:0.1,B:0.2);");

    Assert.assertEquals(Status.INVALID, result.getStatus());
    Assert.assertEquals(Collections.singletonList("Unsupported format: null"), result.getErrors());
  }

  @Test
  public void unsupportedFormatIsInvalidTest() {
    TreeParseResult result =
        PhylogeneticTreeReader.read(
            "tree.xml", "PHYLOXML", "<?xml version=\"1.0\"?><phyloxml><phylogeny/></phyloxml>");

    Assert.assertEquals(Status.INVALID, result.getStatus());
    Assert.assertNull(result.getFormat());
    Assert.assertTrue(result.getTips().isEmpty());
    Assert.assertEquals(
        Collections.singletonList("Unsupported format: PHYLOXML"), result.getErrors());
  }

  @Test
  public void uniqueTreeNameTest() {
    Set<String> used = new HashSet<>();
    Assert.assertEquals("UNTITLED", PhylogeneticTreeReader.uniqueTreeName("* UNTITLED", 1, used));
    Assert.assertEquals("tree2", PhylogeneticTreeReader.uniqueTreeName(null, 2, used));
    Assert.assertEquals("tree3", PhylogeneticTreeReader.uniqueTreeName("  ", 3, used));
    Assert.assertEquals("UNTITLED_4", PhylogeneticTreeReader.uniqueTreeName("UNTITLED", 4, used));
  }

  @Test
  public void uniqueTreeNameSuffixCollisionTest() {
    Set<String> used = new HashSet<>();
    Assert.assertEquals("a", PhylogeneticTreeReader.uniqueTreeName("a", 1, used));
    Assert.assertEquals("a_3", PhylogeneticTreeReader.uniqueTreeName("a_3", 2, used));
    // "a" is taken and "a_3" too
    Assert.assertEquals("a_3_3", PhylogeneticTreeReader.uniqueTreeName("a", 3, used));
  }

  private static Set<String> tipIds(List<TreeTip> tips, String treeName) {
    return tips.stream()
        .filter(t -> treeName.equals(t.getTreeName()))
        .map(TreeTip::getTipId)
        .collect(Collectors.toSet());
  }

  private static Set<String> set(String... values) {
    return new HashSet<>(Arrays.asList(values));
  }
}
