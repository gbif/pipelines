package org.gbif.pipelines.core.parsers.dynamic;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

/**
 * Java version of
 * https://github.com/VertNet/post-harvest-processor/blob/master/lib/vn_utils.py#L1152
 */
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
