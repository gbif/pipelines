package org.gbif.pipelines.core.parsers.dynamic;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class TissueParser {
  private static final Set<String> TISSUE_TOKENS =
      new HashSet<>(
          Arrays.asList(
              "+t",
              "tiss",
              "blood",
              "dmso",
              "dna",
              "extract",
              "froze",
              "forzen",
              "freez",
              "heart",
              "muscle",
              "higado",
              "kidney",
              "liver",
              "lung",
              "nitrogen",
              "pectoral",
              "rinon",
              "riñon",
              "rnalater",
              "sangre",
              "toe",
              "spleen",
              "fin",
              "fetge",
              "cor",
              "teixit"));

  public static boolean hasTissue(String source) {
    if (source == null || source.isEmpty()) {
      return false;
    }
    return TISSUE_TOKENS.stream().anyMatch(source::contains);
  }
}
