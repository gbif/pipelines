package org.gbif.pipelines.common.beam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline.PipelineExecutionException;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class XmlIOTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void oneXmlFilesTest() {

    // State
    final String path =
        getClass()
            .getResource("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a/response.00002.xml")
            .getPath();

    final Map<String, String> coreMap1 = new HashMap<>();
    coreMap1.put(DwcTerm.basisOfRecord.qualifiedName(), "PreservedSpecimen");
    coreMap1.put(DwcTerm.eventDate.qualifiedName(), "1890");
    coreMap1.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    coreMap1.put(DwcTerm.catalogNumber.qualifiedName(), "Ob-0363");
    coreMap1.put(DwcTerm.collectionCode.qualifiedName(), "Ob");
    coreMap1.put(DwcTerm.family.qualifiedName(), "Tenebrionidae");
    coreMap1.put(DwcTerm.order.qualifiedName(), "Coleoptera");
    coreMap1.put(DwcTerm.institutionCode.qualifiedName(), "ZFMK");
    coreMap1.put(DwcTerm.scientificName.qualifiedName(), "Tenebrionidae Latreille, 1802");
    coreMap1.put(DwcTerm.recordedBy.qualifiedName(), "de Vauloger");
    coreMap1.put(DwcTerm.locality.qualifiedName(), "Algeria, Tunisia, Sweden, other countries");
    coreMap1.put(DwcTerm.occurrenceID.qualifiedName(), "ZFMK/Ob/223903199/909708/169734");

    final List<Map<String, String>> multimediaExtension2 = new ArrayList<>();
    multimediaExtension2.add(
        Collections.singletonMap(
            DcTerm.identifier.qualifiedName(),
            "http://biocase.zfmk.de/resource/zfmk/COL/Ob0363.jpg"));
    Map<String, String> mediaItem2 = new HashMap<>();
    mediaItem2.put(
        DcTerm.identifier.qualifiedName(), "http://biocase.zfmk.de/resource/zfmk/COL/Typ5.jpg");
    mediaItem2.put(
        DcTerm.description.qualifiedName(),
        "Pappe - ~25,5 x 18,5 - Oberseite & Seiten: rotbraun - Unterseite: blau - Ränder:\n"
            + "              hellgrün");
    multimediaExtension2.add(mediaItem2);

    final ExtendedRecord expected =
        ExtendedRecord.newBuilder()
            .setId("ZFMK/Ob/223903199/909708/169734")
            .setCoreRowType(DwcTerm.Occurrence.qualifiedName())
            .setCoreTerms(coreMap1)
            .setExtensions(
                Collections.singletonMap(Extension.MULTIMEDIA.getRowType(), multimediaExtension2))
            .build();

    // When
    PCollection<ExtendedRecord> result = p.apply(XmlIO.read(path));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void severalXmlFilesTest() {

    // State
    final String path =
        getClass().getResource("/xml/f0c01a2d-eaa7-4802-bd7f-eb6aad1a4a7a") + "/*.xml";

    final Map<String, String> coreMap1 = new HashMap<>();
    coreMap1.put(DwcTerm.basisOfRecord.qualifiedName(), "PreservedSpecimen");
    coreMap1.put(DwcTerm.eventDate.qualifiedName(), "1890");
    coreMap1.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    coreMap1.put(DwcTerm.catalogNumber.qualifiedName(), "Ob-0363");
    coreMap1.put(DwcTerm.collectionCode.qualifiedName(), "Ob");
    coreMap1.put(DwcTerm.family.qualifiedName(), "Tenebrionidae");
    coreMap1.put(DwcTerm.order.qualifiedName(), "Coleoptera");
    coreMap1.put(DwcTerm.institutionCode.qualifiedName(), "ZFMK");
    coreMap1.put(DwcTerm.scientificName.qualifiedName(), "Tenebrionidae Latreille, 1802");
    coreMap1.put(DwcTerm.recordedBy.qualifiedName(), "de Vauloger");
    coreMap1.put(DwcTerm.locality.qualifiedName(), "Algeria, Tunisia, Sweden, other countries");
    coreMap1.put(DwcTerm.occurrenceID.qualifiedName(), "ZFMK/Ob/223903199/909708/169734");

    final List<Map<String, String>> multimediaExtension1 = new ArrayList<>();
    multimediaExtension1.add(
        Collections.singletonMap(
            DcTerm.identifier.qualifiedName(),
            "http://biocase.zfmk.de/resource/zfmk/COL/Ob0363.jpg"));
    Map<String, String> mediaItem1 = new HashMap<>();
    mediaItem1.put(
        DcTerm.identifier.qualifiedName(), "http://biocase.zfmk.de/resource/zfmk/COL/Typ5.jpg");
    mediaItem1.put(
        DcTerm.description.qualifiedName(),
        "Pappe - ~25,5 x 18,5 - Oberseite & Seiten: rotbraun - Unterseite: blau - Ränder:\n"
            + "              hellgrün");
    multimediaExtension1.add(mediaItem1);

    final ExtendedRecord ex1 =
        ExtendedRecord.newBuilder()
            .setId("ZFMK/Ob/223903199/909708/169734")
            .setCoreRowType(DwcTerm.Occurrence.qualifiedName())
            .setCoreTerms(coreMap1)
            .setExtensions(
                Collections.singletonMap(Extension.MULTIMEDIA.getRowType(), multimediaExtension1))
            .build();

    final Map<String, String> coreMap2 = new HashMap<>();
    coreMap2.put(DwcTerm.basisOfRecord.qualifiedName(), "PreservedSpecimen");
    coreMap2.put(DwcTerm.kingdom.qualifiedName(), "Animalia");
    coreMap2.put(DwcTerm.catalogNumber.qualifiedName(), "Ob-0001");
    coreMap2.put(DwcTerm.collectionCode.qualifiedName(), "Ob");
    coreMap2.put(DwcTerm.family.qualifiedName(), "Curculionidae");
    coreMap2.put(DwcTerm.institutionCode.qualifiedName(), "ZFMK");
    coreMap2.put(DwcTerm.scientificName.qualifiedName(), "Curculionidae Latreille, 1802");
    coreMap2.put(DwcTerm.occurrenceID.qualifiedName(), "ZFMK/Ob/223902837/908708/169372");

    final List<Map<String, String>> multimediaExtension2 = new ArrayList<>();
    multimediaExtension2.add(
        Collections.singletonMap(
            DcTerm.identifier.qualifiedName(),
            "http://biocase.zfmk.de/resource/zfmk/COL/Ob0001.jpg"));
    Map<String, String> mediaItem2 = new HashMap<>();
    mediaItem2.put(
        DcTerm.identifier.qualifiedName(), "http://biocase.zfmk.de/resource/zfmk/COL/Typ1.jpg");
    mediaItem2.put(
        DcTerm.description.qualifiedName(),
        "Pappe - 40 x 30 - Oberseite & Seiten: Holzimitat - Unterseite: schwarz grün mamoriert -\n"
            + "              Ränder: schwarz");
    multimediaExtension2.add(mediaItem2);

    final ExtendedRecord ex2 =
        ExtendedRecord.newBuilder()
            .setId("ZFMK/Ob/223902837/908708/169372")
            .setCoreRowType(DwcTerm.Occurrence.qualifiedName())
            .setCoreTerms(coreMap2)
            .setExtensions(
                Collections.singletonMap(Extension.MULTIMEDIA.getRowType(), multimediaExtension2))
            .build();

    final List<ExtendedRecord> expected = Arrays.asList(ex1, ex2);

    // When
    PCollection<ExtendedRecord> result = p.apply(XmlIO.read(path));

    // Should
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test(expected = PipelineExecutionException.class)
  public void emptyFilesPathTest() {

    // State
    final String path = null;

    // When
    p.apply(XmlIO.read(path));

    // Should
    p.run();
  }
}
