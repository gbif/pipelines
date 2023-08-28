package au.org.ala.pipelines.converters;

import au.org.ala.pipelines.transforms.IndexRecordTransform;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ALAAttributionRecord;
import org.gbif.pipelines.io.avro.ALASensitivityRecord;
import org.gbif.pipelines.io.avro.ALATaxonRecord;
import org.gbif.pipelines.io.avro.ALAUUIDRecord;
import org.gbif.pipelines.io.avro.AgentIdentifier;
import org.gbif.pipelines.io.avro.BasicRecord;
import org.gbif.pipelines.io.avro.Diagnostic;
import org.gbif.pipelines.io.avro.EntityReference;
import org.gbif.pipelines.io.avro.EventDate;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.IndexRecord;
import org.gbif.pipelines.io.avro.LocationRecord;
import org.gbif.pipelines.io.avro.MatchType;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.io.avro.Nomenclature;
import org.gbif.pipelines.io.avro.ParsedName;
import org.gbif.pipelines.io.avro.Rank;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.Status;
import org.gbif.pipelines.io.avro.TaxonProfile;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.TemporalRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.junit.Assert;
import org.junit.Test;

public class MultimediaCsvConverterTest {

  @Test
  public void converterTest() {
    // Expected
    // tr = 1, br = 2, lr = 3, trx = 4, atxr = 5, aur = 6, ir = 7, asr = 8
    String[] expected = {
      "\"aur_uuid\"", // DwcTerm.occurrenceID
      "\"http://image/ir_Identifier\"", // DwcTerm.identifier
      "\"ir_Creator\"", // DcTerm.creator
      "\"ir_Created\"", // DcTerm.created
      "\"ir_Title\"", // DcTerm.title
      "\"ir_Format\"", // DcTerm.format
      "\"ir_License\"", // DcTerm.license
      "\"ir_Rights\"", // DcTerm.rights
      "\"ir_RightsHolder\"", // DcTerm.rightsHolder
      "\"ir_References\"", // DcTerm.references
    };

    // State
    Map<String, String> core = new HashMap<>();
    core.put(DwcTerm.occurrenceID.simpleName(), "raw_er_" + DwcTerm.occurrenceID.simpleName());
    core.put(DwcTerm.catalogNumber.simpleName(), "raw_er_" + DwcTerm.catalogNumber.simpleName());
    core.put(DwcTerm.collectionCode.simpleName(), "raw_er_" + DwcTerm.collectionCode.simpleName());
    core.put(
        DwcTerm.institutionCode.simpleName(), "raw_er_" + DwcTerm.institutionCode.simpleName());
    core.put(DwcTerm.recordNumber.simpleName(), "raw_er_" + DwcTerm.recordNumber.simpleName());
    core.put(DwcTerm.basisOfRecord.simpleName(), "raw_er_" + DwcTerm.basisOfRecord.simpleName());
    core.put(DwcTerm.recordedBy.simpleName(), "raw_er_" + DwcTerm.recordedBy.simpleName());
    core.put(
        DwcTerm.occurrenceStatus.simpleName(), "raw_er_" + DwcTerm.occurrenceStatus.simpleName());
    core.put(
        DwcTerm.individualCount.simpleName(), "raw_er_" + DwcTerm.individualCount.simpleName());
    core.put(DwcTerm.scientificName.simpleName(), "raw_er_" + DwcTerm.scientificName.simpleName());
    core.put(DwcTerm.taxonConceptID.simpleName(), "raw_er_" + DwcTerm.taxonConceptID.simpleName());
    core.put(DwcTerm.taxonRank.simpleName(), "raw_er_" + DwcTerm.taxonRank.simpleName());
    core.put(DwcTerm.kingdom.simpleName(), "raw_er_" + DwcTerm.kingdom.simpleName());
    core.put(DwcTerm.phylum.simpleName(), "raw_er_" + DwcTerm.phylum.simpleName());
    core.put(DwcTerm.class_.simpleName(), "raw_er_" + DwcTerm.class_.simpleName());
    core.put(DwcTerm.order.simpleName(), "raw_er_" + DwcTerm.order.simpleName());
    core.put(DwcTerm.family.simpleName(), "raw_er_" + DwcTerm.family.simpleName());
    core.put(DwcTerm.genus.simpleName(), "raw_er_" + DwcTerm.genus.simpleName());
    core.put(DwcTerm.vernacularName.simpleName(), "raw_er_" + DwcTerm.vernacularName.simpleName());
    core.put(
        DwcTerm.decimalLatitude.simpleName(), "raw_er_" + DwcTerm.decimalLatitude.simpleName());
    core.put(
        DwcTerm.decimalLongitude.simpleName(), "raw_er_" + DwcTerm.decimalLongitude.simpleName());
    core.put(DwcTerm.geodeticDatum.simpleName(), "raw_er_" + DwcTerm.geodeticDatum.simpleName());
    core.put(
        DwcTerm.coordinateUncertaintyInMeters.simpleName(),
        "raw_er_" + DwcTerm.coordinateUncertaintyInMeters.simpleName());
    core.put(
        DwcTerm.maximumElevationInMeters.simpleName(),
        "raw_er_" + DwcTerm.maximumElevationInMeters.simpleName());
    core.put(
        DwcTerm.minimumElevationInMeters.simpleName(),
        "raw_er_" + DwcTerm.minimumElevationInMeters.simpleName());
    core.put(
        DwcTerm.minimumDepthInMeters.simpleName(),
        "raw_er_" + DwcTerm.minimumDepthInMeters.simpleName());
    core.put(
        DwcTerm.maximumDepthInMeters.simpleName(),
        "raw_er_" + DwcTerm.maximumDepthInMeters.simpleName());
    core.put(DwcTerm.country.simpleName(), "raw_er_" + DwcTerm.country.simpleName());
    core.put(DwcTerm.stateProvince.simpleName(), "raw_er_" + DwcTerm.stateProvince.simpleName());
    core.put(DwcTerm.locality.simpleName(), "raw_er_" + DwcTerm.locality.simpleName());
    core.put(
        DwcTerm.locationRemarks.simpleName(), "raw_er_" + DwcTerm.locationRemarks.simpleName());
    core.put(DwcTerm.year.simpleName(), "raw_er_" + DwcTerm.year.simpleName());
    core.put(DwcTerm.month.simpleName(), "raw_er_" + DwcTerm.month.simpleName());
    core.put(DwcTerm.day.simpleName(), "raw_er_" + DwcTerm.day.simpleName());
    core.put(DwcTerm.eventDate.simpleName(), "raw_er_" + DwcTerm.eventDate.simpleName());
    core.put(DwcTerm.eventID.simpleName(), "raw_er_" + DwcTerm.eventID.simpleName());
    core.put(DwcTerm.identifiedBy.simpleName(), "raw_er_" + DwcTerm.identifiedBy.simpleName());
    core.put(
        DwcTerm.occurrenceRemarks.simpleName(), "raw_er_" + DwcTerm.occurrenceRemarks.simpleName());
    core.put(
        DwcTerm.dataGeneralizations.simpleName(),
        "raw_er_" + DwcTerm.dataGeneralizations.simpleName());
    core.put(
        DwcTerm.otherCatalogNumbers.simpleName(),
        "raw_er_" + DwcTerm.otherCatalogNumbers.simpleName());
    core.put(DcTerm.references.simpleName(), "raw_er_" + DcTerm.references.simpleName());

    ExtendedRecord er =
        ExtendedRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCoreTerms(core)
            .build();

    TemporalRecord tr =
        TemporalRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(1L)
            .setDateIdentified("")
            .setCreated(11L)
            .setYear(111)
            .setMonth(1111)
            .setDay(11111)
            .setEventDate(EventDate.newBuilder().setGte("1999").setLte("2000").build())
            .setStartDayOfYear(111111)
            .setEndDayOfYear(1111111)
            .setModified("1999")
            .setDateIdentified("1999")
            .setDatePrecision("1999")
            .build();

    BasicRecord br =
        BasicRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(2L)
            .setBasisOfRecord("br_basisOfRecord")
            .setSex("br_sex")
            .setLifeStage(
                VocabularyConcept.newBuilder()
                    .setConcept("br_lifeStage")
                    .setLineage(Collections.singletonList("br_lifeStageLineage"))
                    .build())
            .setEstablishmentMeans(
                VocabularyConcept.newBuilder()
                    .setConcept("br_establishmentMeans")
                    .setLineage(Collections.singletonList("br_establishmentMeans"))
                    .build())
            .setIndividualCount(222)
            .setTypeStatus(Collections.singletonList("br_typeStatus"))
            .setTypifiedName("br_typifiedName")
            .setSampleSizeValue(222d)
            .setSampleSizeUnit("br_sampleSizeUnit")
            .setOrganismQuantity(2222d)
            .setOrganismQuantityType("br_organismQuantityType")
            .setRelativeOrganismQuantity(22222d)
            .setReferences("br_References")
            .setLicense("br_license")
            .setIdentifiedByIds(
                Collections.singletonList(
                    AgentIdentifier.newBuilder()
                        .setType("br_agent_type")
                        .setValue("br_agent_value")
                        .build()))
            .setRecordedByIds(
                Collections.singletonList(
                    AgentIdentifier.newBuilder()
                        .setType("br_agent_type_rb")
                        .setValue("br_agent_value_rb")
                        .build()))
            .setRecordedBy(Arrays.asList("br_recordedBy_1", "br_recordedBy_2"))
            .setOccurrenceStatus("br_occurrenceStatus")
            .build();

    LocationRecord lr =
        LocationRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setId("lr_id")
            .setCreated(3L)
            .setContinent("lr_continent")
            .setWaterBody("lr_waterBody")
            .setCountry("lr_country")
            .setCountryCode("lr_countryCode")
            .setPublishingCountry("lr_publishingCountry")
            .setStateProvince("lr_stateProvince")
            .setMinimumElevationInMeters(33d)
            .setMaximumElevationInMeters(333d)
            .setElevation(3333d)
            .setElevationAccuracy(33333d)
            .setMinimumDepthInMeters(33333d)
            .setMaximumDepthInMeters(333333d)
            .setDepth(33333333d)
            .setDepthAccuracy(333333333d)
            .setMinimumDistanceAboveSurfaceInMeters(3333333333d)
            .setMaximumDistanceAboveSurfaceInMeters(33333333333d)
            .setDecimalLatitude(333333333333d)
            .setDecimalLongitude(3333333333333d)
            .setCoordinateUncertaintyInMeters(33333333333333d)
            .setCoordinatePrecision(333333333333333d)
            .setHasCoordinate(true)
            .setRepatriated(true)
            .setHasGeospatialIssue(false)
            .setLocality("lr_locality")
            .setGeoreferencedDate("lr_georeferencedDate")
            .setFootprintWKT("lr_footprintWKT")
            .setBiome("lr_biome")
            .build();

    TaxonRecord txr =
        TaxonRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setSynonym(false)
            .setUsage(
                RankedName.newBuilder()
                    .setRank(Rank.SPECIES)
                    .setName("txr_Usage_name")
                    .setKey(4)
                    .build())
            .setClassification(
                Arrays.asList(
                    RankedName.newBuilder()
                        .setRank(Rank.SPECIES)
                        .setName("txr_Classification_SPECIES_name")
                        .setKey(44)
                        .build(),
                    RankedName.newBuilder()
                        .setRank(Rank.CLASS)
                        .setName("txr_Classification_CLASS_name")
                        .setKey(444)
                        .build()))
            .setAcceptedUsage(
                RankedName.newBuilder()
                    .setRank(Rank.SPECIES)
                    .setName("txr_Usage_name")
                    .setKey(4444)
                    .build())
            .setNomenclature(
                Nomenclature.newBuilder()
                    .setId("txr_Nomenclature_id")
                    .setSource("txr_Nomenclature_Source")
                    .build())
            .setDiagnostics(
                Diagnostic.newBuilder()
                    .setConfidence(44444)
                    .setStatus(Status.ACCEPTED)
                    .setNote("txr_Diagnostic_Note")
                    .setMatchType(MatchType.EXACT)
                    .setLineage(Collections.singletonList("txr_Diagnostic_Lineage"))
                    .build())
            .setUsageParsedName(ParsedName.newBuilder().build())
            .setIucnRedListCategoryCode("txr_IucnRedListCategoryCode")
            .build();

    ALATaxonRecord atxr =
        ALATaxonRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setScientificName("atxr_ScientificName")
            .setScientificNameAuthorship("atxr_ScientificNameAuthorship")
            .setTaxonConceptID("atxr_TaxonConceptID")
            .setTaxonRank("atxr_TaxonRank")
            .setTaxonRankID(5)
            .setLft(55)
            .setRgt(555)
            .setMatchType("atxr_MatchType")
            .setNameType("atxr_NameType")
            .setKingdom("atxr_Kingdom")
            .setKingdomID("atxr_KingdomID")
            .setPhylum("atxr_Phylum")
            .setPhylumID("atxr_PhylumID")
            .setClasss("atxr_Classs")
            .setClassID("atxr_ClassID")
            .setOrder("atxr_Order")
            .setOrderID("atxr_OrderID")
            .setFamily("atxr_Family")
            .setFamilyID("atxr_FamilyID")
            .setGenus("atxr_Genus")
            .setGenusID("atxr_GenusID")
            .setSpecies("atxr_Species")
            .setSpeciesID("atxr_SpeciesID")
            .setVernacularName("atxr_VernacularName")
            .setSpeciesGroup(Collections.singletonList("atxr_SpeciesGroup"))
            .setSpeciesSubgroup(Collections.singletonList("atxr_SpeciesSubgroup"))
            .build();

    ALAAttributionRecord aar =
        ALAAttributionRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setDataResourceUid("aar_DataResourceUid")
            .setDataResourceName("aar_DataResourceName")
            .setDataProviderUid("aar_DataProviderUid")
            .setDataProviderName("aar_DataProviderName")
            .setCollectionUid("aar_CollectionUid")
            .setCollectionName("aar_CollectionName")
            .setInstitutionUid("aar_InstitutionUid")
            .setInstitutionName("aar_InstitutionName")
            .setLicenseType("aar_LicenseType")
            .setLicenseVersion("aar_LicenseVersion")
            .setProvenance("aar_Provenance")
            .setHasDefaultValues(false)
            .setHubMembership(
                Collections.singletonList(
                    EntityReference.newBuilder()
                        .setName("aar_EntityReference_name")
                        .setUid("aar_EntityReference_uuid")
                        .setUri("aar_EntityReference_uri")
                        .build()))
            .build();

    ALAUUIDRecord aur =
        ALAUUIDRecord.newBuilder()
            .setId("aur_id")
            .setUuid("aur_uuid")
            .setUniqueKey("aur_uniqueKey")
            .setFirstLoaded(6L)
            .build();

    ImageRecord ir =
        ImageRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(7L)
            .setImageItems(
                Collections.singletonList(
                    Image.newBuilder()
                        .setIdentifier("ir_Identifier")
                        .setCreator("ir_Creator")
                        .setCreated("ir_Created")
                        .setTitle("ir_Title")
                        .setLicense("ir_License")
                        .setFormat("ir_Format")
                        .setRights("ir_Rights")
                        .setRightsHolder("ir_RightsHolder")
                        .setReferences("ir_References")
                        .build()))
            .build();

    TaxonProfile tp = TaxonProfile.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();
    MultimediaRecord mr =
        MultimediaRecord.newBuilder().setId(DwcTerm.occurrenceID.simpleName()).build();

    ALASensitivityRecord asr =
        ALASensitivityRecord.newBuilder()
            .setId(DwcTerm.occurrenceID.simpleName())
            .setCreated(8L)
            .setIsSensitive(false)
            .setSensitive("asr_Sensitive")
            .setDataGeneralizations("asr_DataGeneralizations")
            .setInformationWithheld("asr_InformationWithheld")
            .setGeneralisationToApplyInMetres("asr_GeneralisationToApplyInMetres")
            .setGeneralisationInMetres("asr_GeneralisationInMetres")
            .setOriginal(Collections.singletonMap("asr_Original_key", "asr_Original_value"))
            .setAltered(Collections.singletonMap("asr_Altered_key", "asr_Altered_value"))
            .build();

    Long lastLoadDate = 9L;
    Long lastProcessedDate = 10L;

    IndexRecord source =
        IndexRecordTransform.createIndexRecord(
            br,
            tr,
            lr,
            txr,
            atxr,
            er,
            aar,
            aur,
            ir,
            tp,
            asr,
            mr,
            null,
            null,
            null,
            lastLoadDate,
            lastProcessedDate);

    // When
    List<String> result = MultimediaCsvConverter.convert(source, "http://image/{0}");

    // Should
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(String.join("\t", expected), result.get(0));
  }

  @Test
  public void termListTest() {

    // Expected
    List<String> expected = new LinkedList<>();

    expected.add(DwcTerm.occurrenceID.qualifiedName());
    expected.add(DcTerm.identifier.qualifiedName());
    expected.add(DcTerm.creator.qualifiedName());
    expected.add(DcTerm.created.qualifiedName());
    expected.add(DcTerm.title.qualifiedName());
    expected.add(DcTerm.format.qualifiedName());
    expected.add(DcTerm.license.qualifiedName());
    expected.add(DcTerm.rights.qualifiedName());
    expected.add(DcTerm.rightsHolder.qualifiedName());
    expected.add(DcTerm.references.qualifiedName());

    // When
    List<String> result = MultimediaCsvConverter.getTerms();

    // Should
    Assert.assertEquals(expected.size(), result.size());
    for (int x = 0; x < expected.size(); x++) {
      Assert.assertEquals(expected.get(x), result.get(x));
    }
  }
}
