package org.gbif.pipelines.spark;

import static org.junit.Assert.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.gbif.pipelines.io.avro.MultiTaxonRecord;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.RankedNameWithAuthorship;
import org.gbif.pipelines.io.avro.TaxonRecord;
import org.gbif.pipelines.io.avro.json.TaxonCoverage;
import org.junit.Test;
import scala.Tuple2;

public class CalculateDerivedMetadataTest {

  @Test
  public void testCalculateDerivedMetadataTaxonCoverage() throws Exception {

    SparkSession spark = SparkSession.builder().master("local[*]").getOrCreate();

    ObjectMapper mapper = new ObjectMapper();

    List<Occurrence> occurrences =
        List.of(
            Occurrence.builder()
                .coreId("event-id-1")
                .verbatim("{}")
                .id("1")
                .taxon(
                    multiTaxon(
                        mapper,
                        taxon(
                            "the-dataset-key-uuid",
                            usage("taxon-key-3", "SPECIES", "Panthera leo", "Linnaeus, 1758"),
                            rn("KINGDOM", "Animalia", "taxon-key-1"),
                            rn("GENUS", "Panthera", "taxon-key-2"),
                            rn("SPECIES", "leo", "taxon-key-3")),
                        taxon(
                            "the-dataset-key-uuid-2",
                            usage("taxon-key-2-3", "SPECIES", "Panthera leo", "Linnaeus, 1758"),
                            rn("KINGDOM", "Animalia", "taxon-key-2-1"),
                            rn("GENUS", "Panthera", "taxon-key-2-2"),
                            rn("SPECIES", "leo", "taxon-key-2-3"))))
                .build(),
            Occurrence.builder()
                .coreId("event-id-1")
                .verbatim("{}")
                .id("2")
                .taxon(
                    multiTaxon(
                        mapper,
                        taxon(
                            "the-dataset-key-uuid",
                            usage("taxon-key-4", "SPECIES", "Panthera tigris", "Linnaeus, 1758"),
                            rn("KINGDOM", "Animalia", "taxon-key-1"),
                            rn("GENUS", "Panthera", "taxon-key-2"),
                            rn("SPECIES", "leo", "taxon-key-4")),
                        taxon(
                            "the-dataset-key-uuid-2",
                            usage("taxon-key-2-4", "SPECIES", "Panthera tigris", "Linnaeus, 1758"),
                            rn("KINGDOM", "Animalia", "taxon-key-2-1"),
                            rn("GENUS", "Panthera", "taxon-key-2-2"),
                            rn("SPECIES", "tigris", "taxon-key-2-4"))))
                .build(),
            Occurrence.builder()
                .coreId("event-id-1")
                .verbatim("{}")
                .id("3")
                .taxon(
                    multiTaxon(
                        mapper,
                        taxon(
                            "the-dataset-key-uuid",
                            usage("taxon-key-4", "SPECIES", "Panthera tigris", "Linnaeus, 1758"),
                            rn("KINGDOM", "Animalia", "taxon-key-1"),
                            rn("GENUS", "Panthera", "taxon-key-2"),
                            rn("SPECIES", "leo", "taxon-key-4")),
                        taxon(
                            "the-dataset-key-uuid-2",
                            usage("taxon-key-2-4", "SPECIES", "Panthera tigris", "Linnaeus, 1758"),
                            rn("KINGDOM", "Animalia", "taxon-key-2-1"),
                            rn("GENUS", "Panthera", "taxon-key-2-2"),
                            rn("SPECIES", "tigris", "taxon-key-2-4"))))
                .build());

    Dataset<Occurrence> ds = spark.createDataset(occurrences, Encoders.bean(Occurrence.class));

    URL testRootUrl = getClass().getResource("/");
    assert testRootUrl != null;
    String testResourcesRoot = testRootUrl.getFile();

    Dataset<Tuple2<String, String>> calculatedTaxonCoverage =
        CalculateDerivedMetadata.calculateTaxonomicCoverage(
            testResourcesRoot + "/calculate-derived-data/taxon-coverage", ds);

    Tuple2<String, String> idAndCoverage = calculatedTaxonCoverage.first();

    assertEquals(idAndCoverage._1, "event-id-1");

    ObjectMapper objectMapper = new ObjectMapper();
    TaxonCoverage taxonCoverage = objectMapper.readValue(idAndCoverage._2, TaxonCoverage.class);

    assertEquals(
        Set.of("the-dataset-key-uuid", "the-dataset-key-uuid-2"),
        taxonCoverage.getClassifications().keySet());

    spark.stop();
  }

  private static RankedName rn(String rank, String name, String key) {
    return RankedName.newBuilder().setRank(rank).setName(name).setKey(key).build();
  }

  private static RankedNameWithAuthorship usage(
      String key, String rank, String name, String authorship) {
    return RankedNameWithAuthorship.newBuilder()
        .setKey(key)
        .setRank(rank)
        .setName(name)
        .setAuthorship(authorship)
        .build();
  }

  private static TaxonRecord taxon(
      String datasetKey, RankedNameWithAuthorship usage, RankedName... classification) {

    return TaxonRecord.newBuilder()
        .setDatasetKey(datasetKey)
        .setUsage(usage)
        .setClassification(List.of(classification))
        .build();
  }

  private static String multiTaxon(ObjectMapper mapper, TaxonRecord... records) throws Exception {

    return mapper.writeValueAsString(
        MultiTaxonRecord.newBuilder().setTaxonRecords(List.of(records)).build());
  }
}
