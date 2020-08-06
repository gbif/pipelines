package org.gbif.pipelines.transforms.extension;

import java.util.Collections;
import java.util.Map;
import org.apache.beam.sdk.testing.NeedsRunner;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.gbif.pipelines.transforms.ExtendedRecordCustomBuilder;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@Category(NeedsRunner.class)
public class MultimediaRecordTransformTest {

  private static final String RECORD_ID = "123";
  private static final String URI =
      "http://specify-attachments-saiab.saiab.ac.za/originals/att.JPG";
  private static final String SOURCE = "http://farm8.staticflickr.com/7093/7039524065_8.jpg";
  private static final String TITLE = "Geranium Plume Moth 0032";
  private static final String DESCRIPTION = "Geranium Plume Moth 0032 description";
  private static final String LICENSE =
      "http://creativecommons.org/publicdomain/zero/1.0/legalcode";
  private static final String CREATOR = "Moayed Bahajjaj";
  private static final String CREATED = "2012-03-29";

  @Rule public final transient TestPipeline p = TestPipeline.create();

  private static class CleanDateCreate extends DoFn<MultimediaRecord, MultimediaRecord> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      MultimediaRecord mdr = MultimediaRecord.newBuilder(context.element()).build();
      mdr.setCreated(0L);
      context.output(mdr);
    }
  }

  @Test
  public void transformationTest() {

    // State
    Map<String, String> extension =
        ExtendedRecordCustomBuilder.MultimediaExtensionBuilder.builder()
            .identifier(URI)
            .format("image/jpeg")
            .title(TITLE)
            .description(DESCRIPTION)
            .license(LICENSE)
            .creator(CREATOR)
            .created(CREATED)
            .source(SOURCE)
            .type("image")
            .build()
            .toMap();

    ExtendedRecord extendedRecord =
        ExtendedRecordCustomBuilder.create()
            .id(RECORD_ID)
            .addExtensionRecord(Extension.MULTIMEDIA, extension)
            .build();

    // When
    PCollection<MultimediaRecord> dataStream =
        p.apply(Create.of(extendedRecord))
            .apply(MultimediaTransform.create().interpret())
            .apply("Cleaning timestamps", ParDo.of(new CleanDateCreate()));

    // Should
    PAssert.that(dataStream).containsInAnyOrder(createExpectedMultimedia());
    p.run();
  }

  private MultimediaRecord createExpectedMultimedia() {

    Multimedia multimedia =
        Multimedia.newBuilder()
            .setIdentifier(URI)
            .setFormat("image/jpeg")
            .setTitle(TITLE)
            .setDescription(DESCRIPTION)
            .setLicense("http://creativecommons.org/publicdomain/zero/1.0/legalcode")
            .setCreator(CREATOR)
            .setCreated(CREATED)
            .setSource(SOURCE)
            .setType(MediaType.StillImage.name())
            .build();

    return MultimediaRecord.newBuilder()
        .setId(RECORD_ID)
        .setCreated(0L)
        .setMultimediaItems(Collections.singletonList(multimedia))
        .build();
  }
}
