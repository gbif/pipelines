package org.gbif.pipelines.core.converters;

import java.util.Arrays;
import java.util.Collections;
import org.gbif.pipelines.io.avro.Audubon;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.Image;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.gbif.pipelines.io.avro.IssueRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.io.avro.Multimedia;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.junit.Assert;
import org.junit.Test;

public class MultimediaConverterTest {

  @Test(expected = NullPointerException.class)
  public void nullMergeTest() {
    MultimediaConverter.merge(null, null, null);
  }

  @Test
  public void emptyMergeTest() {

    // State
    MultimediaRecord mr = MultimediaRecord.newBuilder().setId("777").build();
    ImageRecord ir = ImageRecord.newBuilder().setId("777").build();
    AudubonRecord ar = AudubonRecord.newBuilder().setId("777").build();

    MultimediaRecord result = MultimediaRecord.newBuilder().setId("777").build();

    // When
    MultimediaRecord record = MultimediaConverter.merge(mr, ir, ar);

    // Should
    Assert.assertEquals(result, record);
  }

  @Test
  public void multimediaRecordTest() {

    // State
    MultimediaRecord mr =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(
                Collections.singletonList(
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .build()))
            .build();

    ImageRecord ir = ImageRecord.newBuilder().setId("777").build();
    AudubonRecord ar = AudubonRecord.newBuilder().setId("777").build();

    MultimediaRecord result =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(
                Collections.singletonList(
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .build()))
            .build();

    // When
    MultimediaRecord record = MultimediaConverter.merge(mr, ir, ar);

    // Should
    Assert.assertEquals(result, record);
  }

  @Test
  public void duplicateTest() {

    // State
    MultimediaRecord mr =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(
                Collections.singletonList(
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .build()))
            .build();

    ImageRecord ir =
        ImageRecord.newBuilder()
            .setId("777")
            .setImageItems(
                Collections.singletonList(
                    Image.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .build()))
            .build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("777").build();

    MultimediaRecord result =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(
                Collections.singletonList(
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .setType("StillImage")
                        .build()))
            .build();

    // When
    MultimediaRecord record = MultimediaConverter.merge(mr, ir, ar);

    // Should
    Assert.assertEquals(result, record);
  }

  @Test
  public void mergeTest() {

    // State
    MultimediaRecord mr =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(
                Arrays.asList(
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .setCreated("2010-10-10")
                        .setLicense("license1")
                        .build(),
                    Multimedia.newBuilder().setIdentifier("http://url-i3").build()))
            .setIssues(IssueRecord.newBuilder().setIssueList(Arrays.asList("ONE", "THREE")).build())
            .build();

    ImageRecord ir =
        ImageRecord.newBuilder()
            .setId("777")
            .setImageItems(
                Collections.singletonList(
                    Image.newBuilder()
                        .setIdentifier("http://url-i2")
                        .setReferences("http://url-r2")
                        .setCreated("2010-11-11")
                        .setLicense("license2")
                        .build()))
            .setIssues(IssueRecord.newBuilder().setIssueList(Arrays.asList("TWO", "THREE")).build())
            .build();

    AudubonRecord ar =
        AudubonRecord.newBuilder()
            .setId("777")
            .setAudubonItems(
                Collections.singletonList(
                    Audubon.newBuilder()
                        .setAccessUri("http://url-i3")
                        .setCreateDate("2010-09-09")
                        .setRights("license3")
                        .build()))
            .build();

    MultimediaRecord result =
        MultimediaRecord.newBuilder()
            .setId("777")
            .setMultimediaItems(
                Arrays.asList(
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i1")
                        .setReferences("http://url-r1")
                        .setCreated("2010-10-10")
                        .setLicense("license1")
                        .build(),
                    Multimedia.newBuilder()
                        .setType(MediaType.StillImage.name())
                        .setIdentifier("http://url-i2")
                        .setReferences("http://url-r2")
                        .setCreated("2010-11-11")
                        .setLicense("license2")
                        .build(),
                    Multimedia.newBuilder()
                        .setIdentifier("http://url-i3")
                        .setCreated("2010-09-09")
                        .setLicense("license3")
                        .build()))
            .setIssues(
                IssueRecord.newBuilder().setIssueList(Arrays.asList("ONE", "TWO", "THREE")).build())
            .build();

    // When
    MultimediaRecord record = MultimediaConverter.merge(mr, ir, ar);

    // Should
    Assert.assertEquals(result, record);
  }
}
