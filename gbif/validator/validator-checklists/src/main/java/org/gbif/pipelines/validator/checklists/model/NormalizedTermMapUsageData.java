package org.gbif.pipelines.validator.checklists.model;

import static org.gbif.pipelines.validator.checklists.model.ObjectToTermMapper.toTermMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
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

  private static <T> void collectInterpretedExtension(
      List<T> list,
      Extension extension,
      Map<Extension, List<Map<Term, String>>> interpretedExtensions,
      Function<List<T>, List<Map<Term, String>>> mapper) {
    if (list != null && !list.isEmpty()) {
      interpretedExtensions.compute(
          extension,
          (k, v) ->
              interpretedExtensions.compute(
                  extension,
                  (ext, oldValue) -> {
                    if (oldValue == null) {
                      oldValue = new ArrayList<>();
                    }
                    oldValue.addAll(mapper.apply(list));
                    return oldValue;
                  }));
    }
  }

  private static Map<Extension, List<Map<Term, String>>> interpretedExtensions(
      NormalizedNameUsageData normalizedNameUsageData) {

    if (normalizedNameUsageData.getUsageExtensions() != null) {

      Map<Extension, List<Map<Term, String>>> extensions = new HashMap<>();

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().descriptions,
          Extension.DESCRIPTION,
          extensions,
          ObjectToTermMapper::toDescriptionTermMap);

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().distributions,
          Extension.DISTRIBUTION,
          extensions,
          ObjectToTermMapper::toDistributionTermMap);

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().identifiers,
          Extension.IDENTIFIER,
          extensions,
          ObjectToTermMapper::toIdentifierTermMap);

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().media,
          Extension.MULTIMEDIA,
          extensions,
          ObjectToTermMapper::toNameUsageMediaObjectTermMap);

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().referenceList,
          Extension.REFERENCE,
          extensions,
          ObjectToTermMapper::toReferenceTermMap);

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().speciesProfiles,
          Extension.SPECIES_PROFILE,
          extensions,
          ObjectToTermMapper::toSpeciesProfileTermMap);

      collectInterpretedExtension(
          normalizedNameUsageData.getUsageExtensions().vernacularNames,
          Extension.VERNACULAR_NAME,
          extensions,
          ObjectToTermMapper::toVernacularNameTermMap);

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
