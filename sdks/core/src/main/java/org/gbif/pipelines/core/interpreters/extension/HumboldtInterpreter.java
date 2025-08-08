package org.gbif.pipelines.core.interpreters.extension;

import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.INCERTAE_SEDIS;
import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.INCERTAE_SEDIS_KEY;
import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.INCERTAE_SEDIS_NAME;
import static org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter.KINGDOM_RANK;
import static org.gbif.pipelines.core.utils.ModelUtils.extractListValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Strings;
import org.gbif.api.vocabulary.DurationUnit;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.BooleanParser;
import org.gbif.common.parsers.NumberParser;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.EcoTerm;
import org.gbif.dwc.terms.Term;
import org.gbif.kvs.KeyValueStore;
import org.gbif.kvs.species.NameUsageMatchRequest;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.core.TaxonomyInterpreter;
import org.gbif.pipelines.core.interpreters.core.VocabularyInterpreter;
import org.gbif.pipelines.core.parsers.SimpleTypeParser;
import org.gbif.pipelines.core.parsers.humboldt.DurationUnit;
import org.gbif.pipelines.core.parsers.taxonomy.TaxonRecordConverter;
import org.gbif.pipelines.core.parsers.vocabulary.VocabularyService;
import org.gbif.pipelines.core.utils.ModelUtils;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.HumboldtRecord;
import org.gbif.pipelines.io.avro.TaxonHumboldtRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.gbif.rest.client.species.NameUsageMatchResponse;

@Builder(buildMethodName = "create")
@Slf4j
public class HumboldtInterpreter {

  private static final BooleanParser BOOLEAN_PARSER = BooleanParser.getInstance();

  private final KeyValueStore<NameUsageMatchRequest, NameUsageMatchResponse> kvStore;
  private final List<String> checklistKeys;
  private final VocabularyService vocabularyService;

  /**
   * Interprets audubon of a {@link ExtendedRecord} and populates a {@link HumboldtRecord} with the
   * interpreted values.
   */
  public void interpret(ExtendedRecord er, HumboldtRecord hr) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(hr);

    ExtensionInterpretation.Result<Humboldt> result =
        ExtensionInterpretation.extension(Extension.HUMBOLDT)
            .to(() -> Humboldt.newBuilder().build())
            .map(
                EcoTerm.siteCount,
                interpretInt(Humboldt::setSiteCount, OccurrenceIssue.SITE_COUNT_INVALID))
            .map(
                EcoTerm.verbatimSiteDescriptions,
                interpretStringList(Humboldt::setVerbatimSiteDescriptions))
            .map(EcoTerm.verbatimSiteNames, interpretStringList(Humboldt::setVerbatimSiteNames))
            .map(
                EcoTerm.geospatialScopeAreaValue,
                interpretDouble(
                    Humboldt::setGeospatialScopeAreaValue,
                    OccurrenceIssue.GEOSPATIAL_SCOPE_AREA_VALUE_INVALID))
            .map(
                EcoTerm.geospatialScopeAreaUnit,
                interpretString(Humboldt::setGeospatialScopeAreaUnit))
            .map(
                EcoTerm.totalAreaSampledValue,
                interpretDouble(
                    Humboldt::setTotalAreaSampledValue,
                    OccurrenceIssue.TOTAL_AREA_SAMPLED_VALUE_INVALID))
            .map(EcoTerm.totalAreaSampledUnit, interpretString(Humboldt::setTotalAreaSampledUnit))
            .map(EcoTerm.targetHabitatScope, interpretStringList(Humboldt::setTargetHabitatScope))
            .map(
                EcoTerm.excludedHabitatScope,
                interpretStringList(Humboldt::setExcludedHabitatScope))
            .map(
                EcoTerm.eventDurationValue,
                interpretDouble(
                    Humboldt::setEventDurationValue, OccurrenceIssue.EVENT_DURATION_VALUE_INVALID))
            .map(EcoTerm.eventDurationUnit, HumboldtInterpreter::interpretEventDurationUnit)
            .map(
                EcoTerm.targetTaxonomicScope, interpretTaxon(hr, Humboldt::setTargetTaxonomicScope))
            .map(
                EcoTerm.excludedTaxonomicScope,
                interpretTaxon(hr, Humboldt::setExcludedTaxonomicScope))
            .map(
                EcoTerm.taxonCompletenessProtocols,
                interpretStringList(Humboldt::setTaxonCompletenessProtocols))
            .map(
                EcoTerm.isTaxonomicScopeFullyReported,
                interpretBoolean(
                    Humboldt::setIsTaxonomicScopeFullyReported,
                    OccurrenceIssue.IS_TAXONOMIC_SCOPE_FULLY_REPORTED_INVALID))
            .map(
                EcoTerm.isAbsenceReported,
                interpretBoolean(
                    Humboldt::setIsAbsenceReported, OccurrenceIssue.IS_ABSENCE_REPORTED_INVALID))
            .map(EcoTerm.absentTaxa, interpretTaxon(hr, Humboldt::setAbsentTaxa))
            .map(
                EcoTerm.hasNonTargetTaxa,
                interpretBoolean(
                    Humboldt::setHasNonTargetTaxa, OccurrenceIssue.HAS_NON_TARGET_TAXA_INVALID))
            .map(EcoTerm.nonTargetTaxa, interpretTaxon(hr, Humboldt::setNonTargetTaxa))
            .map(
                EcoTerm.areNonTargetTaxaFullyReported,
                interpretBoolean(
                    Humboldt::setAreNonTargetTaxaFullyReported,
                    OccurrenceIssue.ARE_NON_TARGET_TAXA_FULLY_REPORTED_INVALID))
            .map(
                EcoTerm.targetLifeStageScope,
                interpretVocabularyList(DwcTerm.lifeStage, Humboldt::setTargetLifeStageScope))
            .map(
                EcoTerm.excludedLifeStageScope,
                interpretVocabularyList(DwcTerm.lifeStage, Humboldt::setExcludedLifeStageScope))
            .map(
                EcoTerm.isLifeStageScopeFullyReported,
                interpretBoolean(
                    Humboldt::setIsLifeStageScopeFullyReported,
                    OccurrenceIssue.IS_LIFE_STAGE_SCOPE_FULLY_REPORTED_INVALID))
            .map(
                EcoTerm.targetDegreeOfEstablishmentScope,
                interpretVocabularyList(
                    DwcTerm.degreeOfEstablishment, Humboldt::setTargetDegreeOfEstablishmentScope))
            .map(
                EcoTerm.excludedDegreeOfEstablishmentScope,
                interpretVocabularyList(
                    DwcTerm.degreeOfEstablishment, Humboldt::setExcludedDegreeOfEstablishmentScope))
            .map(
                EcoTerm.isDegreeOfEstablishmentScopeFullyReported,
                interpretBoolean(
                    Humboldt::setIsDegreeOfEstablishmentScopeFullyReported,
                    OccurrenceIssue.IS_DEGREE_OF_ESTABLISHMENT_SCOPE_FULLY_REPORTED_INVALID))
            .map(
                EcoTerm.targetGrowthFormScope,
                interpretStringList(Humboldt::setTargetGrowthFormScope))
            .map(
                EcoTerm.excludedGrowthFormScope,
                interpretStringList(Humboldt::setExcludedGrowthFormScope))
            .map(
                EcoTerm.isGrowthFormScopeFullyReported,
                interpretBoolean(
                    Humboldt::setIsGrowthFormScopeFullyReported,
                    OccurrenceIssue.IS_GROWTH_FORM_SCOPE_FULLY_REPORTED_INVALID))
            .map(
                EcoTerm.hasNonTargetOrganisms,
                interpretBoolean(
                    Humboldt::setHasNonTargetOrganisms,
                    OccurrenceIssue.HAS_NON_TARGET_ORGANISMS_INVALID))
            .map(EcoTerm.compilationTypes, interpretStringList(Humboldt::setCompilationTypes))
            .map(
                EcoTerm.compilationSourceTypes,
                interpretStringList(Humboldt::setCompilationSourceTypes))
            .map(EcoTerm.inventoryTypes, interpretStringList(Humboldt::setInventoryTypes))
            .map(EcoTerm.protocolNames, interpretStringList(Humboldt::setProtocolNames))
            .map(
                EcoTerm.protocolDescriptions,
                interpretStringList(Humboldt::setProtocolDescriptions))
            .map(EcoTerm.protocolReferences, interpretStringList(Humboldt::setProtocolReferences))
            .map(
                EcoTerm.isAbundanceReported,
                interpretBoolean(
                    Humboldt::setIsAbundanceReported,
                    OccurrenceIssue.IS_ABUNDANCE_REPORTED_INVALID))
            .map(
                EcoTerm.isAbundanceCapReported,
                interpretBoolean(
                    Humboldt::setIsAbundanceCapReported,
                    OccurrenceIssue.IS_ABUNDANCE_CAP_REPORTED_INVALID))
            .map(
                EcoTerm.abundanceCap,
                interpretInt(Humboldt::setAbundanceCap, OccurrenceIssue.ABUNDANCE_CAP_INVALID))
            .map(
                EcoTerm.isVegetationCoverReported,
                interpretBoolean(
                    Humboldt::setIsVegetationCoverReported,
                    OccurrenceIssue.IS_VEGETATION_COVER_REPORTED_INVALID))
            .map(
                EcoTerm.isLeastSpecificTargetCategoryQuantityInclusive,
                interpretBoolean(
                    Humboldt::setIsLeastSpecificTargetCategoryQuantityInclusive,
                    OccurrenceIssue.IS_LEAST_SPECIFIC_TARGET_CATEGORY_QUANTITY_INCLUSIVE_INVALID))
            .map(
                EcoTerm.hasVouchers,
                interpretBoolean(Humboldt::setHasVouchers, OccurrenceIssue.HAS_VOUCHERS_INVALID))
            .map(EcoTerm.voucherInstitutions, interpretStringList(Humboldt::setVoucherInstitutions))
            .map(
                EcoTerm.hasMaterialSamples,
                interpretBoolean(
                    Humboldt::setHasMaterialSamples, OccurrenceIssue.HAS_MATERIAL_SAMPLES_INVALID))
            .map(EcoTerm.materialSampleTypes, interpretStringList(Humboldt::setMaterialSampleTypes))
            .map(EcoTerm.samplingPerformedBy, interpretStringList(Humboldt::setSamplingPerformedBy))
            .map(
                EcoTerm.isSamplingEffortReported,
                interpretBoolean(
                    Humboldt::setIsSamplingEffortReported,
                    OccurrenceIssue.IS_SAMPLING_EFFORT_REPORTED_INVALID))
            .map(
                EcoTerm.samplingEffortValue,
                interpretDouble(
                    Humboldt::setSamplingEffortValue,
                    OccurrenceIssue.SAMPLING_EFFORT_VALUE_INVALID))
            .map(EcoTerm.samplingEffortUnit, interpretString(Humboldt::setSamplingEffortUnit))
            .postMap(checkAreas())
            .postMap(checkMissingUnits())
            .postMap(checkTargetExclusions())
            .postMap(checkBooleanMismatches())
            .convert(er);

    hr.setHumboldtItems(result.getList());
    if (result.getIssues() != null) {
      hr.getIssues().getIssueList().addAll(result.getIssuesAsList());
    }
  }

  private static Function<Humboldt, List<String>> checkBooleanMismatches() {
    return humboldt -> {
      List<String> issues = new ArrayList<>();
      if (humboldt.getNonTargetTaxa() != null
          && !humboldt.getNonTargetTaxa().isEmpty()
          && Boolean.FALSE.equals(humboldt.getHasNonTargetTaxa())) {
        return List.of(OccurrenceIssue.HAS_NON_TARGET_TAXA_MISMATCH.name());
      }
      if (humboldt.getMaterialSampleTypes() != null
          && !humboldt.getMaterialSampleTypes().isEmpty()
          && Boolean.FALSE.equals(humboldt.getHasMaterialSamples())) {
        return List.of(OccurrenceIssue.HAS_MATERIAL_SAMPLES_MISMATCH.name());
      }
      return issues;
    };
  }

  private static Function<Humboldt, List<String>> checkTargetExclusions() {
    return humboldt -> {
      List<String> issues = new ArrayList<>();

      BiFunction<List<?>, List<?>, Boolean> existExclusion =
          (target, excluded) ->
              target != null
                  && !target.isEmpty()
                  && excluded != null
                  && !excluded.isEmpty()
                  && target.stream().anyMatch(excluded::contains);

      if (existExclusion.apply(
          humboldt.getTargetTaxonomicScope(), humboldt.getExcludedTaxonomicScope())) {
        return List.of(OccurrenceIssue.TARGET_TAXONOMIC_SCOPE_EXCLUDED.name());
      }
      if (existExclusion.apply(
          humboldt.getTargetLifeStageScope(), humboldt.getExcludedLifeStageScope())) {
        return List.of(OccurrenceIssue.TARGET_LIFE_STAGE_SCOPE_EXCLUDED.name());
      }
      if (existExclusion.apply(
          humboldt.getTargetDegreeOfEstablishmentScope(),
          humboldt.getExcludedDegreeOfEstablishmentScope())) {
        return List.of(OccurrenceIssue.TARGET_DEGREE_OF_ESTABLISHMENT_EXCLUDED.name());
      }
      if (existExclusion.apply(
          humboldt.getTargetGrowthFormScope(), humboldt.getExcludedGrowthFormScope())) {
        return List.of(OccurrenceIssue.TARGET_GROWTH_FORM_EXCLUDED.name());
      }
      if (existExclusion.apply(
          humboldt.getTargetHabitatScope(), humboldt.getExcludedHabitatScope())) {
        return List.of(OccurrenceIssue.TARGET_HABITAT_SCOPE_EXCLUDED.name());
      }
      return issues;
    };
  }

  private static Function<Humboldt, List<String>> checkAreas() {
    return humboldt -> {
      if (humboldt.getGeospatialScopeAreaValue() != null
          && humboldt.getTotalAreaSampledValue() != null
          // we only check them if they have the same unit since we can't interpret the units until
          // we have a vocabulary
          && humboldt
              .getGeospatialScopeAreaUnit()
              .equalsIgnoreCase(humboldt.getTotalAreaSampledUnit())
          && humboldt.getGeospatialScopeAreaValue() < humboldt.getTotalAreaSampledValue()) {
        return List.of(OccurrenceIssue.GEOSPATIAL_SCOPE_AREA_LOWER_THAN_TOTAL_AREA_SAMPLED.name());
      }
      return List.of();
    };
  }

  private static Function<Humboldt, List<String>> checkMissingUnits() {
    List<String> issues = new ArrayList<>();
    return humboldt -> {
      if (humboldt.getEventDurationValue() != null && humboldt.getEventDurationUnit() == null) {
        issues.add(OccurrenceIssue.EVENT_DURATION_UNIT_MISSING.name());
      }
      if (humboldt.getGeospatialScopeAreaValue() != null
          && humboldt.getGeospatialScopeAreaUnit() == null) {
        issues.add(OccurrenceIssue.GEOSPATIAL_SCOPE_AREA_UNIT_MISSING.name());
      }
      if (humboldt.getSamplingEffortValue() != null && humboldt.getSamplingEffortUnit() == null) {
        issues.add(OccurrenceIssue.SAMPLING_EFFORT_UNIT_MISSING.name());
      }
      if (humboldt.getTotalAreaSampledValue() != null
          && humboldt.getTotalAreaSampledUnit() == null) {
        issues.add(OccurrenceIssue.TOTAL_AREA_SAMPLED_UNIT_MISSING.name());
      }
      return issues;
    };
  }

  private static BiFunction<Humboldt, String, List<String>> interpretBoolean(
      BiConsumer<Humboldt, Boolean> setter, OccurrenceIssue issue) {
    return (humboldt, rawValue) -> {
      List<String> issues = new ArrayList<>();
      if (!Strings.isNullOrEmpty(rawValue)) {
        Boolean result = BOOLEAN_PARSER.parse(rawValue).getPayload();
        if (result != null) {
          setter.accept(humboldt, Boolean.parseBoolean(rawValue));
        } else {
          issues.add(issue.name());
        }
      }
      return issues;
    };
  }

  private static BiConsumer<Humboldt, String> interpretString(BiConsumer<Humboldt, String> setter) {
    return (humboldt, rawValue) -> {
      if (ModelUtils.hasValue(rawValue)) {
        setter.accept(humboldt, rawValue);
      }
    };
  }

  private static BiConsumer<Humboldt, String> interpretStringList(
      BiConsumer<Humboldt, List<String>> setter) {
    return (humboldt, rawValue) -> {
      List<String> list = extractListValue(rawValue);
      if (!list.isEmpty()) {
        setter.accept(humboldt, list);
      }
    };
  }

  private static BiFunction<Humboldt, String, List<String>> interpretDouble(
      BiConsumer<Humboldt, Double> setter, OccurrenceIssue issue) {
    return (humboldt, rawValue) -> {
      List<String> issues = new ArrayList<>();
      Consumer<Optional<Double>> fn =
          parseResult -> {
            Double result = parseResult.orElse(null);
            if (result != null && result >= 0) {
              setter.accept(humboldt, result);
            } else {
              issues.add(issue.name());
            }
          };

      SimpleTypeParser.parseDouble(rawValue, fn);
      return issues;
    };
  }

  private static BiFunction<Humboldt, String, List<String>> interpretInt(
      BiConsumer<Humboldt, Integer> setter, OccurrenceIssue issue) {
    return (humboldt, rawValue) -> {
      List<String> issues = new ArrayList<>();
      if (!Strings.isNullOrEmpty(rawValue)) {
        Integer parsed = NumberParser.parseInteger(rawValue);
        if (parsed != null && parsed >= 0) {
          setter.accept(humboldt, parsed);
        } else {
          issues.add(issue.name());
        }
      }
      return issues;
    };
  }

  private static void interpretEventDurationUnit(Humboldt humboldt, String rawValue) {
    DurationUnit.parseDurationUnit(rawValue)
        .ifPresent(d -> humboldt.setEventDurationUnit(d.name()));
  }

  private BiConsumer<Humboldt, String> interpretTaxon(
      HumboldtRecord humboldtRecord, BiConsumer<Humboldt, List<TaxonHumboldtRecord>> setter) {
    return (humboldt, rawValue) -> {
      final List<TaxonHumboldtRecord> taxonRecords = new ArrayList<>();
      extractListValue(rawValue)
          .forEach(
              value -> {
                if (kvStore == null) {
                  return;
                }

                for (String checklistKey : checklistKeys) {
                  final NameUsageMatchRequest nameUsageMatchRequest =
                      NameUsageMatchRequest.builder().withScientificName(value).build();

                  TaxonHumboldtRecord thr =
                      TaxonHumboldtRecord.newBuilder().setChecklistKey(checklistKey).build();
                  taxonRecords.add(thr);

                  TaxonomyInterpreter.matchTaxon(
                      nameUsageMatchRequest,
                      kvStore,
                      thr,
                      response -> {
                        thr.setUsageRank(KINGDOM_RANK);
                        thr.setUsageName(INCERTAE_SEDIS_NAME);
                        thr.setUsageKey(INCERTAE_SEDIS_KEY);
                        thr.setClassification(Collections.singletonList(INCERTAE_SEDIS));
                      },
                      response -> {
                        if (response.getUsage() != null) {
                          thr.setUsageName(response.getUsage().getName());
                          thr.setUsageKey(String.valueOf(response.getUsage().getKey()));
                          thr.setUsageRank(response.getUsage().getRank());
                        }

                        if (response.getClassification() != null) {
                          thr.setClassification(
                              response.getClassification().stream()
                                  .map(TaxonRecordConverter::convertRankedName)
                                  .collect(Collectors.toList()));
                        }
                      });
                }
              });
      setter.accept(humboldt, taxonRecords);
    };
  }

  private BiConsumer<Humboldt, String> interpretVocabularyList(
      Term term, BiConsumer<Humboldt, List<VocabularyConcept>> setter) {
    return (humboldt, rawValue) -> {
      List<VocabularyConcept> concepts = new ArrayList<>();
      extractListValue(rawValue)
          .forEach(
              v ->
                  VocabularyInterpreter.interpretVocabulary(term, v, vocabularyService, null)
                      .ifPresent(concepts::add));
      if (!concepts.isEmpty()) {
        setter.accept(humboldt, concepts);
      }
    };
  }
}
