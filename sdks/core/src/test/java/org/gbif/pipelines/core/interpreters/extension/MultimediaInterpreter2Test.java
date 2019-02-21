package org.gbif.pipelines.core.interpreters.extension;

import java.util.Arrays;
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

public class MultimediaInterpreter2Test {

  @Test
  public void multimediaTest() {

    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DcTerm.references.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.identifier.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.type.qualifiedName(), "image");
    ext1.put(DcTerm.format.qualifiedName(), "text/plain");
    ext1.put(DcTerm.created.qualifiedName(), "21/11/2000");
    ext1.put(DcTerm.title.qualifiedName(), "Title1");
    ext1.put(DcTerm.description.qualifiedName(), "Desc1");
    ext1.put(DcTerm.contributor.qualifiedName(), "Cont1");
    ext1.put(DcTerm.publisher.qualifiedName(), "Publ1");
    ext1.put(DcTerm.audience.qualifiedName(), "Audi1");
    ext1.put(DcTerm.license.qualifiedName(), "Lice1");
    ext1.put(DwcTerm.datasetID.qualifiedName(), "1");
    ext1.put(DcTerm.rightsHolder.qualifiedName(), "Rh1");
    ext1.put(DcTerm.creator.qualifiedName(), "Cr1");
    ext1.put(DcTerm.source.qualifiedName(), "Sr1");

    Map<String, String> ext2 = new HashMap<>();
    ext2.put(DcTerm.references.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext2.put(DcTerm.identifier.qualifiedName(), "www.gbif.org/tmp.jpg");
    ext2.put(DcTerm.type.qualifiedName(), "audio");
    ext2.put(DcTerm.format.qualifiedName(), "text/plain");
    ext2.put(DcTerm.created.qualifiedName(), "2010");
    ext2.put(DcTerm.title.qualifiedName(), "Title2");
    ext2.put(DcTerm.description.qualifiedName(), "Desc2");
    ext2.put(DcTerm.contributor.qualifiedName(), "Cont2");
    ext2.put(DcTerm.publisher.qualifiedName(), "Pub2");
    ext2.put(DcTerm.audience.qualifiedName(), "Aud2");
    ext2.put(DcTerm.license.qualifiedName(), "Lice2");
    ext2.put(DwcTerm.datasetID.qualifiedName(), "2");
    ext2.put(DcTerm.rightsHolder.qualifiedName(), "Rh2");
    ext2.put(DcTerm.creator.qualifiedName(), "Cr2");
    ext2.put(DcTerm.source.qualifiedName(), "Sr2");

    Map<String, String> ext3 = new HashMap<>();
    ext3.put(DcTerm.created.qualifiedName(), "not a date");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.MULTIMEDIA.getRowType(), Arrays.asList(ext1, ext2, ext3));

    ExtendedRecord record = ExtendedRecord.newBuilder()
        .setId("id")
        .setExtensions(ext)
        .build();

    String result =
        "{\"id\": \"id\", \"multimediaItems\": [{\"type\": \"StillImage\", \"format\": \"text/plain\", \"identifier\": "
            + "\"http://www.gbif.org/tmp.jpg\", \"references\": \"http://www.gbif.org/tmp.jpg\", \"title\": \"Title1\", "
            + "\"description\": \"Desc1\", \"source\": \"Sr1\", \"audience\": \"Audi1\", \"created\": \"2000-11-21\", "
            + "\"creator\": \"Cr1\", \"contributor\": \"Cont1\", \"publisher\": \"Publ1\", \"license\": \"Lice1\", "
            + "\"rightsHolder\": \"Rh1\", \"datasetId\": \"1\"}, {\"type\": \"Sound\", \"format\": \"text/plain\", "
            + "\"identifier\": \"http://www.gbif.org/tmp.jpg\", \"references\": \"http://www.gbif.org/tmp.jpg\", "
            + "\"title\": \"Title2\", \"description\": \"Desc2\", \"source\": \"Sr2\", \"audience\": \"Aud2\", "
            + "\"created\": \"2010\", \"creator\": \"Cr2\", \"contributor\": \"Cont2\", \"publisher\": \"Pub2\", "
            + "\"license\": \"Lice2\", \"rightsHolder\": \"Rh2\", \"datasetId\": \"2\"}], \"issues\": {\"issueList\": "
            + "[\"RECORDED_DATE_INVALID\", \"MULTIMEDIA_URI_INVALID\"]}}";

    MultimediaRecord mr = MultimediaRecord.newBuilder().setId(record.getId()).build();

    // When
    MultimediaInterpreter2.interpret(record, mr);

    //Should
    Assert.assertEquals(result, mr.toString());
  }

}
