package au.org.ala.pipelines.transforms;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.NonNull;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;
import org.gbif.pipelines.core.converters.JsonConverter;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.OccurrenceSearchRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;

/**
 * This converter will take AVRO record artefacts related to an event and produce a single AVRO
 * document that will support Spark SQL searching.
 */
@SuppressWarnings("ConstantConditions")
@Builder
public class ALAOccurrenceToSearchTransform implements Serializable {

  private static final long serialVersionUID = 1279313941024805871L;
  // Core
  @NonNull private final TupleTag<ExtendedRecord> verbatimRecordTag;
  @NonNull private final TupleTag<ALAUUIDRecord> alaUuidRecordTTag;
  @NonNull private final TupleTag<BasicRecord> basicRecordTag;
  @NonNull private final TupleTag<TemporalRecord> temporalRecordTag;
  @NonNull private final TupleTag<ALAAttributionRecord> alaAttributionTag;
  @NonNull private final TupleTag<LocationRecord> locationRecordTag;
  @NonNull private final TupleTag<ALATaxonRecord> taxonRecordTag;
  @NonNull private final TupleTag<ALASensitivityRecord> sensitivityRecordTag;

  // Extension
  public SingleOutput<KV<String, CoGbkResult>, OccurrenceSearchRecord> converter() {

    DoFn<KV<String, CoGbkResult>, OccurrenceSearchRecord> fn =
        new DoFn<KV<String, CoGbkResult>, OccurrenceSearchRecord>() {

          @ProcessElement
          public void processElement(ProcessContext c) {
            CoGbkResult v = c.element().getValue();
            String k = c.element().getKey();

            ExtendedRecord er =
                v.getOnly(verbatimRecordTag, ExtendedRecord.newBuilder().setId(k).build());
            ALAUUIDRecord uur =
                v.getOnly(alaUuidRecordTTag, ALAUUIDRecord.newBuilder().setId(k).build());
            ALAAttributionRecord ar =
                v.getOnly(alaAttributionTag, ALAAttributionRecord.newBuilder().setId(k).build());
            BasicRecord br = v.getOnly(basicRecordTag, BasicRecord.newBuilder().setId(k).build());
            // Core
            TemporalRecord tr =
                v.getOnly(temporalRecordTag, TemporalRecord.newBuilder().setId(k).build());
            LocationRecord lr =
                v.getOnly(locationRecordTag, LocationRecord.newBuilder().setId(k).build());
            ALATaxonRecord txr =
                v.getOnly(taxonRecordTag, ALATaxonRecord.newBuilder().setId(k).build());
            ALASensitivityRecord sdr =
                v.getOnly(sensitivityRecordTag, ALASensitivityRecord.newBuilder().setId(k).build());

            // for now, exclude sensitive records from the exports
            if (sdr != null && sdr.getIsSensitive() != null && !sdr.getIsSensitive()) {

              // Convert and
              OccurrenceSearchRecord.Builder builder =
                  OccurrenceSearchRecord.newBuilder().setId(br.getId());

              // set IDs - core ID will be eventID for event based datasets
              // setting this is required for the joins
              builder.setCoreId(br.getCoreId());
              builder.setOccurrenceID(uur.getUuid());

              // metadata
              builder.setDatasetKey(ar.getDataResourceUid());
              builder.setDataResourceName(ar.getDataResourceName());
              builder.setDataProviderName(ar.getDataProviderName());
              builder.setCollectionName(ar.getCollectionName());
              builder.setInstitutionName(ar.getInstitutionName());

              // copy basic fields
              builder.setBasisOfRecord(br.getBasisOfRecord());
              builder.setSex(br.getSex());
              builder.setLifeStage(
                  br.getLifeStage() != null ? br.getLifeStage().getConcept() : null);
              builder.setEstablishmentMeans(
                  br.getEstablishmentMeans() != null
                      ? br.getEstablishmentMeans().getConcept()
                      : null);
              builder.setDegreeOfEstablishment(
                  br.getDegreeOfEstablishment() != null
                      ? br.getDegreeOfEstablishment().getConcept()
                      : null);
              builder.setPathway(br.getPathway() != null ? br.getPathway().getConcept() : null);
              builder.setIndividualCount(br.getIndividualCount());
              builder.setTypeStatus(br.getTypeStatus());
              builder.setTypifiedName(br.getTypifiedName());
              builder.setSampleSizeValue(br.getSampleSizeValue());
              builder.setSampleSizeUnit(br.getSampleSizeUnit());
              builder.setOrganismQuantity(br.getOrganismQuantity());
              builder.setOrganismQuantityType(br.getOrganismQuantityType());
              builder.setRelativeOrganismQuantity(br.getRelativeOrganismQuantity());
              builder.setReferences(br.getReferences());
              builder.setLicense(br.getLicense());
              builder.setIdentifiedByIds(br.getIdentifiedByIds());
              builder.setIdentifiedBy(br.getIdentifiedBy());
              builder.setRecordedByIds(br.getRecordedByIds());
              builder.setRecordedBy(br.getRecordedBy());
              builder.setOccurrenceStatus(br.getOccurrenceStatus());
              builder.setDatasetID(br.getDatasetID());
              builder.setDatasetName(br.getDatasetName());
              builder.setOtherCatalogNumbers(br.getOtherCatalogNumbers());
              builder.setPreparations(br.getPreparations());
              builder.setSamplingProtocol(br.getSamplingProtocol());
              // taxon fields
              builder.setScientificName(txr.getScientificName());
              builder.setScientificNameAuthorship(txr.getScientificNameAuthorship());
              builder.setTaxonConceptID(txr.getTaxonConceptID());
              builder.setTaxonRank(txr.getTaxonRank());
              builder.setTaxonRankID(txr.getTaxonRankID());
              builder.setMatchType(txr.getMatchType());
              builder.setNameType(txr.getMatchType());
              builder.setKingdom(txr.getKingdom());
              builder.setKingdomID(txr.getKingdomID());
              builder.setPhylum(txr.getPhylum());
              builder.setPhylumID(txr.getPhylumID());
              builder.setClasss(txr.getClasss());
              builder.setClassID(txr.getClassID());
              builder.setOrder(txr.getOrder());
              builder.setOrderID(txr.getOrderID());
              builder.setFamily(txr.getFamily());
              builder.setFamilyID(txr.getFamilyID());
              builder.setGenus(txr.getGenus());
              builder.setGenusID(txr.getGenusID());
              builder.setSpecies(txr.getSpecies());
              builder.setSpeciesID(txr.getSpeciesID());
              builder.setVernacularName(txr.getVernacularName());
              builder.setSpeciesGroup(txr.getSpeciesGroup());
              builder.setSpeciesSubgroup(txr.getSpeciesSubgroup());
              // copy location fields
              builder.setContinent(lr.getContinent());
              builder.setWaterBody(lr.getWaterBody());
              builder.setCountry(lr.getCountry());
              builder.setMinimumElevationInMeters(lr.getMinimumElevationInMeters());
              builder.setMaximumElevationInMeters(lr.getMinimumElevationInMeters());
              builder.setElevation(lr.getElevation());
              builder.setElevationAccuracy(lr.getElevationAccuracy());
              builder.setMinimumDepthInMeters(lr.getMinimumDepthInMeters());
              builder.setMaximumDepthInMeters(lr.getMaximumDepthInMeters());
              builder.setDepth(lr.getDepth());
              builder.setDepthAccuracy(lr.getDepthAccuracy());
              builder.setMinimumDistanceAboveSurfaceInMeters(
                  lr.getMinimumDistanceAboveSurfaceInMeters());
              builder.setMaximumDistanceAboveSurfaceInMeters(
                  lr.getMaximumDistanceAboveSurfaceInMeters());
              builder.setDecimalLatitude(lr.getDecimalLatitude());
              builder.setDecimalLongitude(lr.getDecimalLongitude());
              builder.setCoordinateUncertaintyInMeters(lr.getCoordinateUncertaintyInMeters());
              builder.setCoordinatePrecision(lr.getCoordinatePrecision());
              builder.setLocality(lr.getLocality());
              builder.setGeoreferencedDate(lr.getGeoreferencedDate());
              builder.setFootprintWKT(lr.getFootprintWKT());
              builder.setBiome(lr.getBiome());
              // copy temporal fields
              builder.setDay(tr.getDay());
              builder.setEventDate(JsonConverter.convertEventDateSingle(tr).orElse(null));
              builder.setStartDayOfYear(tr.getStartDayOfYear());
              builder.setEndDayOfYear(tr.getEndDayOfYear());
              builder.setDateIdentified(tr.getDateIdentified());
              builder.setDatePrecision(tr.getDatePrecision());

              List<String> issues = new ArrayList<String>();
              // concat temporal, taxonomic etc
              issues.addAll(br.getIssues().getIssueList());
              issues.addAll(tr.getIssues().getIssueList());
              issues.addAll(lr.getIssues().getIssueList());
              issues.addAll(txr.getIssues().getIssueList());
              builder.setIssues(issues);

              // add verbatim occurrence as field for downstream export
              builder.setVerbatim(er.getCoreTerms());
              c.output(builder.build());
            }
          }
        };
    return ParDo.of(fn);
  }
}
