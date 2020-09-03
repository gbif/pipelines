package org.gbif.pipelines.core.interpreters.extension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.MultimediaRecord;
import org.junit.Assert;
import org.junit.Test;

public class MultimediaInterpreterTest {

  @Test
  public void multimediaTest() {

    // State
    Map<String, String> ext1 = new HashMap<>(14);
    ext1.put(DcTerm.references.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.identifier.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.format.qualifiedName(), "image/scan");
    ext1.put(DcTerm.created.qualifiedName(), "21/11/2000");
    ext1.put(DcTerm.title.qualifiedName(), "Title1");
    ext1.put(DcTerm.description.qualifiedName(), "Desc1");
    ext1.put(DcTerm.contributor.qualifiedName(), "Cont1");
    ext1.put(DcTerm.publisher.qualifiedName(), "Publ1");
    ext1.put(DcTerm.audience.qualifiedName(), "Audi1");
    ext1.put(DcTerm.license.qualifiedName(), "http://creativecommons.org/licenses/by-nc-sa/4.0/");
    ext1.put(DwcTerm.datasetID.qualifiedName(), "1");
    ext1.put(DcTerm.rightsHolder.qualifiedName(), "Rh1");
    ext1.put(DcTerm.creator.qualifiedName(), "Cr1");
    ext1.put(DcTerm.source.qualifiedName(), "Sr1");

    Map<String, String> ext2 = new HashMap<>(14);
    ext2.put(DcTerm.references.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext2.put(DcTerm.identifier.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext2.put(DcTerm.format.qualifiedName(), "jpg");
    ext2.put(DcTerm.created.qualifiedName(), "2010");
    ext2.put(DcTerm.title.qualifiedName(), "Title2");
    ext2.put(DcTerm.description.qualifiedName(), "Desc2");
    ext2.put(DcTerm.contributor.qualifiedName(), "Cont2");
    ext2.put(DcTerm.publisher.qualifiedName(), "Pub2");
    ext2.put(DcTerm.audience.qualifiedName(), "Aud2");
    ext2.put(
        DcTerm.license.qualifiedName(),
        "http://creativecommons.org/publicdomain/zero/1.0/legalcode");
    ext2.put(DwcTerm.datasetID.qualifiedName(), "2");
    ext2.put(DcTerm.rightsHolder.qualifiedName(), "Rh2");
    ext2.put(DcTerm.creator.qualifiedName(), "Cr2");
    ext2.put(DcTerm.source.qualifiedName(), "Sr2");

    Map<String, String> ext3 = new HashMap<>(1);
    ext3.put(DcTerm.created.qualifiedName(), "2021-01-12T18:33:58.000+0000");

    Map<String, String> ext4 = new HashMap<>(4);
    ext3.put(
        DcTerm.identifier.qualifiedName(),
        "https://quod.lib.umich.edu/cgi/i/image/api/image/herb00ic:1559372:MICH-V-1559372/full/res:0/0/native.jpg");

    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    ext.put(Extension.MULTIMEDIA.getRowType(), Arrays.asList(ext1, ext2, ext3, ext4));

    ExtendedRecord record =
        ExtendedRecord.newBuilder()
            .setId("id")
            .setCoreTerms(
                Collections.singletonMap(
                    DwcTerm.associatedMedia.qualifiedName(), "www.gbif.org/tmp22.jpg"))
            .setExtensions(ext)
            .build();

    String result =
        "{\"id\": \"id\", \"created\": 0, \"multimediaItems\": [{\"type\": \"StillImage\", \"format\": \"image/scan\", "
            + "\"identifier\": \"http://www.gbif.org/tmp.jpg\", \"references\": \"http://www.gbif.org/tmp.jpg\", \"title\": "
            + "\"Title1\", \"description\": \"Desc1\", \"source\": \"Sr1\", \"audience\": \"Audi1\", \"created\": \"2000-11-21\", "
            + "\"creator\": \"Cr1\", \"contributor\": \"Cont1\", \"publisher\": \"Publ1\", \"license\": "
            + "\"http://creativecommons.org/licenses/by-nc-sa/4.0/\", \"rightsHolder\": \"Rh1\", \"datasetId\": \"1\"}, {\"type\": "
            + "\"StillImage\", \"format\": \"image/jpeg\", \"identifier\": \"http://www.gbif.org/tmp.jpg\", \"references\": "
            + "\"http://www.gbif.org/tmp.jpg\", \"title\": \"Title2\", \"description\": \"Desc2\", \"source\": \"Sr2\", "
            + "\"audience\": \"Aud2\", \"created\": \"2010\", \"creator\": \"Cr2\", \"contributor\": \"Cont2\", \"publisher\": "
            + "\"Pub2\", \"license\": \"http://creativecommons.org/publicdomain/zero/1.0/legalcode\", \"rightsHolder\": \"Rh2\", \"datasetId\": \"2\"}, {\"type\": \"StillImage\", "
            + "\"format\": \"image/jpeg\", \"identifier\": \"https://quod.lib.umich.edu/cgi/i/image/api/image/herb00ic:1559372:MICH-V-1559372/full/res:0/0/native.jpg\", "
            + "\"references\": null, \"title\": null, \"description\": null, \"source\": null, \"audience\": null, \"created\": null, "
            + "\"creator\": null, \"contributor\": null, \"publisher\": null, \"license\": null, \"rightsHolder\": null, \"datasetId\": null}, "
            + "{\"type\": \"StillImage\", \"format\": \"image/jpeg\", \"identifier\": \"http://www.gbif.org/tmp22.jpg\", \"references\": null, "
            + "\"title\": null, \"description\": null, \"source\": null, \"audience\": null, \"created\": null, \"creator\": null, \"contributor\": null, "
            + "\"publisher\": null, \"license\": null, \"rightsHolder\": null, \"datasetId\": null}], \"issues\": {\"issueList\": "
            + "[\"MULTIMEDIA_DATE_INVALID\", \"MULTIMEDIA_URI_INVALID\"]}}";

    MultimediaRecord mr =
        MultimediaRecord.newBuilder().setId(record.getId()).setCreated(0L).build();

    // When
    MultimediaInterpreter.interpret(record, mr);
    MultimediaInterpreter.interpretAssociatedMedia(record, mr);

    // Should
    Assert.assertEquals(result, mr.toString());
  }
}
