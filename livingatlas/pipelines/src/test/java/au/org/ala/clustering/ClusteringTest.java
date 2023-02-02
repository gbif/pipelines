package au.org.ala.clustering;

import au.org.ala.pipelines.beam.ClusteringPipeline;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.gbif.pipelines.core.parsers.clustering.OccurrenceRelationships;
import org.gbif.pipelines.core.parsers.clustering.RelationshipAssertion;
import org.gbif.pipelines.io.avro.Relationship;
import org.junit.Assert;
import org.junit.Test;

public class ClusteringTest {

  @Test
  public void testAmRecords() {

    // urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,a83e6e60-9f1d-442e-822a-b53e4dae41d1,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41907.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41907.001,ecatalogue.irn:2217397; urn:catalog:AM:Mammalogy:M.41907.001
    // urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,77d5a441-a674-4c90-a344-b8862b18410a,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41956.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41956.001,ecatalogue.irn:2217459; urn:catalog:AM:Mammalogy:M.41956.001
    // urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,9b1d32b8-01fb-4e5c-92fb-1b908c9b9cb1,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41918.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41918.001,ecatalogue.irn:2217468; urn:catalog:AM:Mammalogy:M.41918.001
    // urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,6ca4917e-36e9-4067-bd29-4de87f1e303c,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41902.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41902.001,ecatalogue.irn:2217345; urn:catalog:AM:Mammalogy:M.41902.001

    // 6ca4917e-36e9-4067-bd29-4de87f1e303c
    HashKeyOccurrence h1 =
        createFromString(
            "urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,a83e6e60-9f1d-442e-822a-b53e4dae41d1,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41907.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41907.001,ecatalogue.irn:2217397; urn:catalog:AM:Mammalogy:M.41907.001");
    HashKeyOccurrence h2 =
        createFromString(
            "urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,77d5a441-a674-4c90-a344-b8862b18410a,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41956.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41956.001,ecatalogue.irn:2217459; urn:catalog:AM:Mammalogy:M.41956.001");
    HashKeyOccurrence h3 =
        createFromString(
            "urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,9b1d32b8-01fb-4e5c-92fb-1b908c9b9cb1,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41918.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41918.001,ecatalogue.irn:2217468; urn:catalog:AM:Mammalogy:M.41918.001");
    HashKeyOccurrence h4 =
        createFromString(
            "urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26,6ca4917e-36e9-4067-bd29-4de87f1e303c,dr340,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,Pteropus alecto,AU,urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12,PRESERVED_SPECIMEN,-12.38091,130.85902,1994,9,26,,null,null,null,null,M.41902.001,urn:lsid:ozcam.taxonomy.org.au:AM:Mammalogy:M.41902.001,ecatalogue.irn:2217345; urn:catalog:AM:Mammalogy:M.41902.001");

    RelationshipAssertion<HashKeyOccurrence> assertion = OccurrenceRelationships.generate(h1, h2);
    Assert.assertNotNull(assertion);
    Assert.assertNotNull(
        assertion.justificationContains(
            RelationshipAssertion.FeatureAssertion.SAME_ACCEPTED_SPECIES));
    Assert.assertNotNull(
        assertion.justificationContains(RelationshipAssertion.FeatureAssertion.SAME_COORDINATES));
    Assert.assertNotNull(
        assertion.justificationContains(RelationshipAssertion.FeatureAssertion.SAME_COUNTRY));

    // assertion related
    Assert.assertNotNull(OccurrenceRelationships.generate(h1, h2));
    Assert.assertNotNull(OccurrenceRelationships.generate(h2, h3));
    Assert.assertNotNull(OccurrenceRelationships.generate(h3, h4));
    Assert.assertNotNull(OccurrenceRelationships.generate(h1, h3));
    Assert.assertNotNull(OccurrenceRelationships.generate(h1, h4));
    Assert.assertNotNull(OccurrenceRelationships.generate(h2, h4));

    List<HashKeyOccurrence> candidates = new ArrayList<>();
    candidates.add(h1);
    candidates.add(h2);
    candidates.add(h3);
    candidates.add(h4);

    ClusteringCandidates cc =
        ClusteringCandidates.builder()
            .hashKey(
                "urn:lsid:biodiversity.org.au:afd.taxon:9b8ca2d0-3524-4e12-a328-9a426b31cd12|-12381|130859|1994|9|26")
            .candidates(candidates)
            .build();

    List<KV<String, Relationship>> kvs2 = ClusteringPipeline.createRelationships(cc, 50);
    Assert.assertNotEquals(kvs2.size(), 0);
  }

  private HashKeyOccurrence createFromString(String str) {
    String[] parts = Splitter.on(',').splitToList(str).toArray(new String[0]);
    return HashKeyOccurrenceBuilder.aHashKeyOccurrence()
        .withHashKey(parts[0])
        .withId(parts[1])
        .withDatasetKey(parts[2])
        .withSpeciesKey(parts[3])
        .withScientificName(parts[4])
        .withCountryCode(parts[5])
        .withTaxonKey(parts[6])
        .withBasisOfRecord(parts[7])
        .withDecimalLatitude(Double.valueOf(parts[8]))
        .withDecimalLongitude(Double.valueOf(parts[9]))
        .withYear(Integer.valueOf(parts[10]))
        .withMonth(Integer.valueOf(parts[11]))
        .withDay(Integer.valueOf(parts[12]))
        .withEventDate(parts[13])
        .withTypeStatus(Lists.newArrayList(parts[14]))
        .withRecordedBy(Lists.newArrayList(parts[15]))
        .withFieldNumber(parts[16])
        .withRecordNumber(parts[17])
        .withCatalogNumber(parts[18])
        .withOccurrenceID(parts[19])
        .withOtherCatalogNumbers(Lists.newArrayList(parts[20]))
        .build();
  }
}
