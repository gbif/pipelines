package org.gbif.pipelines.core.utils;

import java.util.List;
import org.gbif.pipelines.core.pojo.HumboldtJsonView;
import org.gbif.pipelines.io.avro.Humboldt;
import org.gbif.pipelines.io.avro.RankedName;
import org.gbif.pipelines.io.avro.TaxonHumboldtRecord;
import org.gbif.pipelines.io.avro.VocabularyConcept;
import org.junit.Test;

public class MediaSerDeserTest {

  @Test
  public void humboldtItemsTest() {
    Humboldt humboldt =
        Humboldt.newBuilder()
            .setTargetTaxonomicScope(
                List.of(
                    TaxonHumboldtRecord.newBuilder()
                        .setClassification(
                            List.of(
                                RankedName.newBuilder().setRank("rank").setName("name").build()))
                        .build()))
            .setTargetLifeStageScope(
                List.of(
                    VocabularyConcept.newBuilder()
                        .setConcept("c1")
                        .setLineage(List.of("c0", "c1"))
                        .build(),
                    VocabularyConcept.newBuilder()
                        .setConcept("c11")
                        .setLineage(List.of("c00", "c11"))
                        .build()))
            .build();

    HumboldtJsonView jsonView = new HumboldtJsonView();
    jsonView.setHumboldt(humboldt);

    HumboldtJsonView.VocabularyList lifeStage = new HumboldtJsonView.VocabularyList();
    lifeStage.setLineage(List.of("c1", "c2"));
    lifeStage.setConcepts(List.of("c1", "c2"));
    jsonView.setTargetLifeStageScope(lifeStage);

    System.out.println(MediaSerDeser.humboldtToJson(List.of(jsonView)));
  }
}
