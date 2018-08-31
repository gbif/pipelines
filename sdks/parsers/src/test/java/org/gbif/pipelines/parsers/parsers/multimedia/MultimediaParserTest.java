package org.gbif.pipelines.parsers.parsers.multimedia;

import org.gbif.api.vocabulary.Extension;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MediaType;
import org.gbif.pipelines.parsers.parsers.common.ParsedField;
import org.gbif.pipelines.parsers.utils.ExtendedRecordBuilder;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_URI_INVALID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** Tests the {@link MultimediaParser}. */
public class MultimediaParserTest {

  @Test
  public void audubonExtensionTest() {

    // State
    Map<String, String> audubonExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
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
        ExtendedRecordBuilder.create()
            .id("123")
            .addExtensionRecord(Extension.AUDUBON, audubonExtension)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);
    ParsedMultimedia multimediaResult = result.getResult().get(0);

    // Should
    assertTrue(result.isSuccessful());
    assertEquals(0, result.getIssues().size());
    assertEquals(1, result.getResult().size());
    assertEquals("description", multimediaResult.getDescription());
    assertEquals("source", multimediaResult.getSource());
    assertEquals(MediaType.StillImage, multimediaResult.getType());
  }

  @Test
  public void multipleExtensionsTest() {

    // State
    Map<String, String> audubonExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .accessURI("http://uri.com")
            .identifier("http://identifier.com")
            .references("http://references.com")
            .title("audubon")
            .build();

    Map<String, String> multimediaExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .accessURI("http://uri.com")
            .identifier("http://identifier.com")
            .references("http://references.com")
            .title("multimedia")
            .build();

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .addExtensionRecord(Extension.AUDUBON, audubonExtension)
            .addExtensionRecord(Extension.MULTIMEDIA, multimediaExtension)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);

    // Should
    assertTrue(result.isSuccessful());
    assertEquals(0, result.getIssues().size());
    assertEquals(1, result.getResult().size());
    assertEquals("multimedia", result.getResult().get(0).getTitle());
  }

  @Test
  public void oneExtensionsTest() {

    // State
    Map<String, String> audubonExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .accessURI("http://uri.com")
            .identifier("http://identifier.com")
            .references("http://references.com")
            .title("audubon")
            .build();

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .addExtensionRecord(Extension.AUDUBON, audubonExtension)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);

    // Should
    assertTrue(result.isSuccessful());
    assertEquals(0, result.getIssues().size());
    assertEquals(1, result.getResult().size());
    assertEquals("audubon", result.getResult().get(0).getTitle());
  }

  @Test
  public void multimediaCoreTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .associatedMedia("http://uri1.com,http://uri2.com")
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);

    // Should
    assertTrue(result.isSuccessful());
    assertEquals(0, result.getIssues().size());
    assertEquals(2, result.getResult().size());
    assertEquals("http://uri1.com", result.getResult().get(0).getReferences().toString());
    assertEquals("http://uri2.com", result.getResult().get(1).getReferences().toString());
  }

  @Test
  public void issuesTest() {

    // State
    Map<String, String> audubonExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder().title("test").build();

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .addExtensionRecord(Extension.AUDUBON, audubonExtension)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);

    // Should
    assertFalse(result.isSuccessful());
    assertEquals(1, result.getIssues().size());
    assertEquals(MULTIMEDIA_URI_INVALID.name(), result.getIssues().get(0));
  }

  @Test
  public void interpretMediaCoreTest() {

    // State
    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .associatedMedia(
                "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg, http://www.flickr.com/photos/70939559@N02/7039524065.png")
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);

    // Should
    assertEquals(2, result.getResult().size());
    for (ParsedMultimedia media : result.getResult()) {
      assertEquals(MediaType.StillImage, media.getType());
      assertTrue(media.getFormat().startsWith("image/"));
      assertNotNull(media.getIdentifier());
    }
  }

  @Test
  public void interpretMediaExtensionTest() {

    // State
    Map<String, String> imageExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .identifier("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
            .references("http://www.flickr.com/photos/70939559@N02/7039524065")
            .format("jpg")
            .title("Geranium Plume Moth 0032")
            .description("Geranium Plume Moth 0032 description")
            .license("BY-NC-SA 2.0")
            .creator("Moayed Bahajjaj")
            .created("2012-03-29")
            .build();

    Map<String, String> imageExtension2 =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .identifier("http://www.flickr.com/photos/70939559@N02/7039524065.jpg")
            .build();

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .addExtensionRecord(Extension.IMAGE, imageExtension)
            .addExtensionRecord(Extension.IMAGE, imageExtension2)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);
    ParsedMultimedia multimediaResult = result.getResult().get(0);

    // Should
    assertEquals(2, result.getResult().size());
    for (ParsedMultimedia media : result.getResult()) {
      assertEquals(MediaType.StillImage, media.getType());
      assertNotNull(media.getIdentifier());
    }
    assertEquals("image/jpeg", multimediaResult.getFormat());
    assertEquals("Geranium Plume Moth 0032", multimediaResult.getTitle());
    assertEquals("Geranium Plume Moth 0032 description", multimediaResult.getDescription());
    assertEquals("BY-NC-SA 2.0", multimediaResult.getLicense());
    assertEquals("Moayed Bahajjaj", multimediaResult.getCreator());
    assertEquals("2012-03-29", multimediaResult.getCreated().toString());
    assertEquals(
        "http://www.flickr.com/photos/70939559@N02/7039524065",
        multimediaResult.getReferences().toString());
    assertEquals(
        "http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg",
        multimediaResult.getIdentifier().toString());
  }

  @Test
  public void interpretAudubonExtensionTest() {

    // State
    Map<String, String> audubonExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .accessURI(
                "http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG")
            .identifier("d79633d3-0967-40fa-9557-d6915e4d1353")
            .format("jpg")
            .title("Geranium Plume Moth 0032")
            .description("Geranium Plume Moth 0032 description")
            .derivedFrom("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
            .license("BY-NC-SA 2.0")
            .creator("Moayed Bahajjaj")
            .created("2012-03-29")
            .build();

    Map<String, String> audubonExtension2 =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .identifier("http://www.flickr.com/photos/70939559@N02/7039524065.jpg")
            .build();

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .addExtensionRecord(Extension.AUDUBON, audubonExtension)
            .addExtensionRecord(Extension.AUDUBON, audubonExtension2)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);
    ParsedMultimedia multimedia = result.getResult().get(1);

    // Should
    assertEquals(2, result.getResult().size());
    for (ParsedMultimedia media : result.getResult()) {
      assertEquals(MediaType.StillImage, media.getType());
      assertNotNull(media.getIdentifier());
    }
    assertEquals("image/jpeg", multimedia.getFormat());
    assertEquals("Geranium Plume Moth 0032", multimedia.getTitle());
    assertEquals("Geranium Plume Moth 0032 description", multimedia.getDescription());
    assertEquals("BY-NC-SA 2.0", multimedia.getLicense());
    assertEquals("Moayed Bahajjaj", multimedia.getCreator());
    assertEquals("2012-03-29", multimedia.getCreated().toString());
    assertEquals(
        "http://specify-attachments-saiab.saiab.ac.za/originals/sp6-3853933608872243693.att.JPG",
        multimedia.getIdentifier().toString());
  }

  /**
   * If the information about the same image (same URI) is given in the org.gbif.pipelines.core AND
   * the extension we should use the one from the extension (richer data).
   */
  @Test
  public void extensionsPriorityTest() {

    // State
    Map<String, String> audubonExtension =
        ExtendedRecordBuilder.createMultimediaExtensionBuilder()
            .accessURI("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
            .format("jpg")
            .title("Geranium Plume Moth 0032")
            .description("Geranium Plume Moth 0032 description")
            .derivedFrom("http://farm8.staticflickr.com/7093/7039524065_3ed0382368.jpg")
            .license("BY-NC-SA 2.0")
            .creator("Moayed Bahajjaj")
            .created("2012-03-29")
            .build();

    ExtendedRecord extendedRecord =
        ExtendedRecordBuilder.create()
            .id("123")
            .associatedMedia(
                "http://farm8.staticflickr"
                    + ".com/7093/7039524065_3ed0382368.jpg, http://www.flickr.com/photos/70939559@N02/7039524065.png")
            .addExtensionRecord(Extension.AUDUBON, audubonExtension)
            .build();

    // When
    ParsedField<List<ParsedMultimedia>> result = MultimediaParser.parseMultimedia(extendedRecord);

    // Should
    assertEquals(2, result.getResult().size());
    assertEquals("BY-NC-SA 2.0", result.getResult().get(0).getLicense());
  }
}
