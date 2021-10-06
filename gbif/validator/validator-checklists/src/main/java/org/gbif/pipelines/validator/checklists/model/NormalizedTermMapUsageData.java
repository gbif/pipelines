package org.gbif.pipelines.validator.checklists.model;

import static org.gbif.pipelines.validator.checklists.model.ObjectToTermMapper.toTermMap;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.Term;

@Data
@Builder
/** Object holder for the results of Checklists normalization. */
public class NormalizedTermMapUsageData {

  private final Map<Term, String> verbatimNameUsage;

  private final Map<Term, String> interpretedNameUsage;

  private final Map<Extension, List<Map<Term, String>>> verbatimExtensions;

  private final Map<Extension, List<Map<Term, String>>> interpretedExtensions;

  private static Map<Term, String> interpretedTermMap(
      NormalizedNameUsageData normalizedNameUsageData) {

    Map<Term, String> termMap = toTermMap(normalizedNameUsageData.getNameUsage());

    if (normalizedNameUsageData.getParsedName() != null) {
      termMap.putAll(toTermMap(normalizedNameUsageData.getParsedName()));
    }
    return termMap;
  }

  private static Map<Extension, List<Map<Term, String>>> interpretedExtensions(
      NormalizedNameUsageData normalizedNameUsageData) {

    if (normalizedNameUsageData.getUsageExtensions() != null) {

      Map<Extension, List<Map<Term, String>>> extensions = new HashMap<>();
      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().descriptions)
          .ifPresent(
              e ->
                  extensions.put(
                      Extension.DESCRIPTION, ObjectToTermMapper.toDescriptionTermMap(e)));

      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().distributions)
          .ifPresent(
              e ->
                  extensions.put(
                      Extension.DISTRIBUTION, ObjectToTermMapper.toDistributionTermMap(e)));

      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().identifiers)
          .ifPresent(
              e -> extensions.put(Extension.IDENTIFIER, ObjectToTermMapper.toIdentifierTermMap(e)));

      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().media)
          .ifPresent(
              e ->
                  extensions.put(
                      Extension.MULTIMEDIA, ObjectToTermMapper.toNameUsageMediaObjectTermMap(e)));

      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().referenceList)
          .ifPresent(
              e -> extensions.put(Extension.REFERENCE, ObjectToTermMapper.toReferenceTermMap(e)));

      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().speciesProfiles)
          .ifPresent(
              e ->
                  extensions.put(
                      Extension.SPECIES_PROFILE, ObjectToTermMapper.toSpeciesProfileTermMap(e)));

      Optional.ofNullable(normalizedNameUsageData.getUsageExtensions().vernacularNames)
          .ifPresent(
              e ->
                  extensions.put(
                      Extension.VERNACULAR_NAME, ObjectToTermMapper.toVernacularNameTermMap(e)));

      return extensions;
    }
    return Collections.emptyMap();
  }

  public static NormalizedTermMapUsageData of(NormalizedNameUsageData normalizedNameUsageData) {
    NormalizedTermMapUsageData.NormalizedTermMapUsageDataBuilder builder =
        NormalizedTermMapUsageData.builder();
    if (normalizedNameUsageData.getVerbatimNameUsage() != null) {
      builder
          .verbatimNameUsage(normalizedNameUsageData.getVerbatimNameUsage().getFields())
          .verbatimExtensions(normalizedNameUsageData.getVerbatimNameUsage().getExtensions());
    }
    return builder
        .interpretedNameUsage(interpretedTermMap(normalizedNameUsageData))
        .interpretedExtensions(interpretedExtensions(normalizedNameUsageData))
        .build();
  }
}
