package org.gbif.pipelines.core.interpreters.extension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.api.vocabulary.Extension;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.gbif.pipelines.io.avro.ImageRecord;
import org.junit.Assert;
import org.junit.Test;

public class ImageInterpreterTest {

  @Test
  public void imageTest() {
    // State
    Map<String, String> ext1 = new HashMap<>();
    ext1.put(DcTerm.identifier.qualifiedName(), "http://www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.references.qualifiedName(), "http://www.gbif.org/tmp.jpg");
    ext1.put(DcTerm.created.qualifiedName(), "2010");
    ext1.put(DcTerm.title.qualifiedName(), "Tt1");
    ext1.put(DcTerm.description.qualifiedName(), "Desc1");
    ext1.put(DcTerm.spatial.qualifiedName(), "Sp1");
    ext1.put(DcTerm.format.qualifiedName(), "jpeg");
    ext1.put(DcTerm.creator.qualifiedName(), "Cr1");
    ext1.put(DcTerm.contributor.qualifiedName(), "Cont1");
    ext1.put(DcTerm.publisher.qualifiedName(), "Pub1");
    ext1.put(DcTerm.audience.qualifiedName(), "Aud1");
    ext1.put(DcTerm.license.qualifiedName(), "Lic1");
    ext1.put(DcTerm.rightsHolder.qualifiedName(), "Rh1");
    ext1.put(DwcTerm.datasetID.qualifiedName(), "1");
    ext1.put("http://www.w3.org/2003/01/geo/wgs84_pos#longitude", "-131.3");
    ext1.put("http://www.w3.org/2003/01/geo/wgs84_pos#latitude", "60.4");

    Map<String, String> ext2 = new HashMap<>();
    ext2.put(DcTerm.identifier.qualifiedName(), "http://www.gbif.org/tmp.jpg");
    ext2.put(DcTerm.references.qualifiedName(), "http://www.gbif.org/tmp.jpg");
    ext2.put(DcTerm.created.qualifiedName(), "2010/12/12");
    ext2.put(DcTerm.title.qualifiedName(), "Tt2");
    ext2.put(DcTerm.description.qualifiedName(), "Desc2");
    ext2.put(DcTerm.spatial.qualifiedName(), "Sp2");
    ext2.put(DcTerm.format.qualifiedName(), "jpeg");
    ext2.put(DcTerm.creator.qualifiedName(), "Cr2");
    ext2.put(DcTerm.contributor.qualifiedName(), "Cont2");
    ext2.put(DcTerm.publisher.qualifiedName(), "Pub2");
    ext2.put(DcTerm.audience.qualifiedName(), "Aud2");
    ext2.put(DcTerm.license.qualifiedName(), "Lic2");
    ext2.put(DcTerm.rightsHolder.qualifiedName(), "Rh2");
    ext2.put(DwcTerm.datasetID.qualifiedName(), "1");
    ext2.put("http://www.w3.org/2003/01/geo/wgs84_pos#longitude", "-131.3");
    ext2.put("http://www.w3.org/2003/01/geo/wgs84_pos#latitude", "360.4");

    Map<String, String> ext3 = new HashMap<>();
    ext3.put(DcTerm.created.qualifiedName(), "not a date");

    Map<String, List<Map<String, String>>> ext = new HashMap<>();
    ext.put(Extension.IMAGE.getRowType(), Arrays.asList(ext1, ext2, ext3));

    ExtendedRecord record = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    String result =
        "{\"id\": \"id\", \"created\": 0, \"imageItems\": [{\"identifier\": \"http://www.gbif.org/tmp.jpg\", \"references\": "
            + "\"http://www.gbif.org/tmp.jpg\", \"title\": \"Tt1\", \"description\": \"Desc1\", \"spatial\": \"Sp1\", "
            + "\"latitude\": 60.4, \"longitude\": -131.3, \"format\": \"jpeg\", \"created\": \"2010\", \"creator\": "
            + "\"Cr1\", \"contributor\": \"Cont1\", \"publisher\": \"Pub1\", \"audience\": \"Aud1\", \"license\": "
            + "\"Lic1\", \"rightsHolder\": \"Rh1\", \"datasetId\": \"1\"}, {\"identifier\": \"http://www.gbif.org/tmp.jpg\", "
            + "\"references\": \"http://www.gbif.org/tmp.jpg\", \"title\": \"Tt2\", \"description\": \"Desc2\", \"spatial\": "
            + "\"Sp2\", \"latitude\": -131.3, \"longitude\": 360.4, \"format\": \"jpeg\", \"created\": \"2010-12-12\", "
            + "\"creator\": \"Cr2\", \"contributor\": \"Cont2\", \"publisher\": \"Pub2\", \"audience\": \"Aud2\", \"license\": "
            + "\"Lic2\", \"rightsHolder\": \"Rh2\", \"datasetId\": \"1\"}], \"issues\": {\"issueList\": ["
            + "\"MULTIMEDIA_DATE_INVALID\", \"MULTIMEDIA_URI_INVALID\"]}}";

    ImageRecord ir = ImageRecord.newBuilder().setId(record.getId()).setCreated(0L).build();

    // When
    ImageInterpreter.interpret(record, ir);

    // Should
    Assert.assertEquals(result, ir.toString());
  }
}
