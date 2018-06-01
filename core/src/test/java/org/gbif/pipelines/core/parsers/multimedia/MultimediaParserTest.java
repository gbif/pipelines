package org.gbif.pipelines.core.parsers.multimedia;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.core.parsers.common.ParsedField;
import org.gbif.pipelines.core.utils.ExtendedRecordCustomBuilder;
import org.gbif.pipelines.io.avro.record.ExtendedRecord;
import org.gbif.pipelines.io.avro.record.issue.IssueType;
import org.gbif.pipelines.io.avro.record.multimedia.MediaType;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests the {@link MultimediaParser}.
 */
public class MultimediaParserTest {

  @Test
  public void audubonExtensionSuccesfulTest() {

    Map<String, String> audubonExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .accessURI("http://uri.com")
      .references("http://references.com")
      .title("title")
      .description("description")
      .caption("caption")
      .license("license")
      .publisher("publisher")
      .contributor("contributor")
      .source("source")
      .derivedFrom("derivedFrom")
      .audience("audience")
      .rightsHolder("rightsHolder")
      .creator("creator")
      .format("image/jpeg")
      .created("10/10/2000")
      .build();

    ExtendedRecord extendedRecord =
      ExtendedRecordCustomBuilder.create().id("123").addExtensionRecord(Extension.AUDUBON, audubonExtension).build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertTrue(parsedMultimedia.isSuccessful());
    assertEquals(0, parsedMultimedia.getIssues().size());
    assertEquals(1, parsedMultimedia.getResult().size());

    // assert field values that check several terms
    ParsedMultimedia multimedia = parsedMultimedia.getResult().get(0);
    assertEquals("description", multimedia.getDescription());
    assertEquals("source", multimedia.getSource());
    assertEquals(MediaType.StillImage, multimedia.getType());
  }

  @Test
  public void multipleExtensionsTest() {
    // using 2 extensions
    Map<String, String> audubonExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .accessURI("http://uri.com")
      .identifier("http://identifier.com")
      .references("http://references.com")
      .title("audubon")
      .build();

    Map<String, String> multimediaExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .accessURI("http://uri.com")
      .identifier("http://identifier.com")
      .references("http://references.com")
      .title("multimedia")
      .build();

    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create()
      .id("123")
      .addExtensionRecord(Extension.AUDUBON, audubonExtension)
      .addExtensionRecord(Extension.MULTIMEDIA, multimediaExtension)
      .build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertTrue(parsedMultimedia.isSuccessful());
    assertEquals(0, parsedMultimedia.getIssues().size());

    // it should use multimedia extension
    assertEquals(1, parsedMultimedia.getResult().size());
    assertEquals("multimedia", parsedMultimedia.getResult().get(0).getTitle());

    // using 1 extension
    extendedRecord =
      ExtendedRecordCustomBuilder.create().id("123").addExtensionRecord(Extension.AUDUBON, audubonExtension).build();

    parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertTrue(parsedMultimedia.isSuccessful());
    assertEquals(0, parsedMultimedia.getIssues().size());

    // it should use multimedia extension
    assertEquals(1, parsedMultimedia.getResult().size());
    assertEquals("audubon", parsedMultimedia.getResult().get(0).getTitle());
  }

  @Test
  public void multimediaCoreTest() {
    ExtendedRecord extendedRecord =
      ExtendedRecordCustomBuilder.create().id("123").associatedMedia("http://uri1.com,http://uri2.com").build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);
    assertTrue(parsedMultimedia.isSuccessful());
    assertEquals(0, parsedMultimedia.getIssues().size());
    assertEquals(2, parsedMultimedia.getResult().size());

    assertEquals("http://uri1.com", parsedMultimedia.getResult().get(0).getReferences().toString());
    assertEquals("http://uri2.com", parsedMultimedia.getResult().get(1).getReferences().toString());
  }

  @Test
  public void issuesTest() {
    Map<String, String> audubonExtension =
      ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder().title("test").build();

    ExtendedRecord extendedRecord =
      ExtendedRecordCustomBuilder.create().id("123").addExtensionRecord(Extension.AUDUBON, audubonExtension).build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertFalse(parsedMultimedia.isSuccessful());
    assertEquals(1, parsedMultimedia.getIssues().size());
    assertEquals(IssueType.MULTIMEDIA_URI_INVALID, parsedMultimedia.getIssues().get(0).getIssueType());
  }

  @Test
  public void testInterpretMediaCore() {
    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create()
      .id("123")
      .associatedMedia(
        "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg, http://www.flickr.com/photos/70939559@N02/7039524065.png")
      .build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertEquals(2, parsedMultimedia.getResult().size());
    for (ParsedMultimedia media : parsedMultimedia.getResult()) {
      assertEquals(MediaType.StillImage, media.getType());
      assertTrue(media.getFormat().startsWith("image/"));
      assertNotNull(media.getIdentifier());
    }
  }

  @Test
  public void testInterpretMediaExtension() {

    Map<String, String> imageExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .identifier("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
      .references("http://www.flickr.com/photos/70939559@N02/7039524065")
      .format("jpg")
      .title("Geranium Plume Moth 0032")
      .description("Geranium Plume Moth 0032 description")
      .license("BY-NC-SA 2.0")
      .creator("Moayed Bahajjaj")
      .created("2012-03-29")
      .build();

    Map<String, String> imageExtension2 = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .identifier("http://www.flickr.com/photos/70939559@N02/7039524065.jpg")
      .build();

    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create()
      .id("123")
      .addExtensionRecord(Extension.IMAGE, imageExtension)
      .addExtensionRecord(Extension.IMAGE, imageExtension2)
      .build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertEquals(2, parsedMultimedia.getResult().size());
    for (ParsedMultimedia media : parsedMultimedia.getResult()) {
      assertEquals(MediaType.StillImage, media.getType());
      assertNotNull(media.getIdentifier());
    }

    ParsedMultimedia multimedia = parsedMultimedia.getResult().get(0);
    assertEquals("image/jpeg", multimedia.getFormat());
    assertEquals("Geranium Plume Moth 0032", multimedia.getTitle());
    assertEquals("Geranium Plume Moth 0032 description", multimedia.getDescription());
    assertEquals("BY-NC-SA 2.0", multimedia.getLicense());
    assertEquals("Moayed Bahajjaj", multimedia.getCreator());
    assertEquals("2012-03-29", multimedia.getCreated().toString());
    assertEquals("http://www.flickr.com/photos/70939559@N02/7039524065", multimedia.getReferences().toString());
    assertEquals("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg", multimedia.getIdentifier().toString());
  }

  @Test
  public void testInterpretAudubonExtension() {

    Map<String, String> audubonExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .accessURI("http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG")
      .identifier("d79633d3-0967-40fa-9557-d6915e4d1353")
      .format("jpg")
      .title("Geranium Plume Moth 0032")
      .description("Geranium Plume Moth 0032 description")
      .derivedFrom("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
      .license("BY-NC-SA 2.0")
      .creator("Moayed Bahajjaj")
      .created("2012-03-29")
      .build();

    Map<String, String> audubonExtension2 = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .identifier("http://www.flickr.com/photos/70939559@N02/7039524065.jpg")
      .build();

    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create()
      .id("123")
      .addExtensionRecord(Extension.AUDUBON, audubonExtension)
      .addExtensionRecord(Extension.AUDUBON, audubonExtension2)
      .build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertEquals(2, parsedMultimedia.getResult().size());
    for (ParsedMultimedia media : parsedMultimedia.getResult()) {
      assertEquals(MediaType.StillImage, media.getType());
      assertNotNull(media.getIdentifier());
    }

    ParsedMultimedia multimedia = parsedMultimedia.getResult().get(1);
    assertEquals("image/jpeg", multimedia.getFormat());
    assertEquals("Geranium Plume Moth 0032", multimedia.getTitle());
    assertEquals("Geranium Plume Moth 0032 description", multimedia.getDescription());
    assertEquals("BY-NC-SA 2.0", multimedia.getLicense());
    assertEquals("Moayed Bahajjaj", multimedia.getCreator());
    assertEquals("2012-03-29", multimedia.getCreated().toString());
    assertEquals("http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG",
                 multimedia.getIdentifier().toString());
  }

  /**
   * If the information about the same image (same URI) is given in the core AND the extension we should use the one
   * from the extension (richer data).
   */
  @Test
  public void testExtensionsPriority() {
    Map<String, String> audubonExtension = ExtendedRecordCustomBuilder.createMultimediaExtensionBuilder()
      .accessURI("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
      .format("jpg")
      .title("Geranium Plume Moth 0032")
      .description("Geranium Plume Moth 0032 description")
      .derivedFrom("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
      .license("BY-NC-SA 2.0")
      .creator("Moayed Bahajjaj")
      .created("2012-03-29")
      .build();

    ExtendedRecord extendedRecord = ExtendedRecordCustomBuilder.create()
      .id("123")
      .associatedMedia("http://farm8.staticflickr"
                       + ".com/7093/7039524065_3ed0382368.jpg, http://www.flickr.com/photos/70939559@N02/7039524065.png")
      .addExtensionRecord(Extension.AUDUBON, audubonExtension)
      .build();

    ParsedField<List<ParsedMultimedia>> parsedMultimedia = MultimediaParser.parseMultimedia(extendedRecord);

    assertEquals(2, parsedMultimedia.getResult().size());
    assertEquals("BY-NC-SA 2.0", parsedMultimedia.getResult().get(0).getLicense());
  }

}
