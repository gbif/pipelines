package org.gbif.pipelines.ingest.hdfs.converters;

import org.gbif.api.vocabulary.License;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.pipelines.io.avro.*;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class OccurrenceHdfsRecordConverterTest {

    @Test
    public void extendedRecordConverterTest() {
        Map<String,String> coreTerms = new HashMap<>();
        coreTerms.put(DwcTerm.verbatimDepth.simpleName(),"1.0");
        coreTerms.put(DwcTerm.collectionCode.simpleName(),"C1");
        coreTerms.put(DwcTerm.institutionCode.simpleName(),"I1");
        coreTerms.put(DwcTerm.catalogNumber.simpleName(), "CN1");
        ExtendedRecord extendedRecord = ExtendedRecord.newBuilder()
                                         .setId("1")
                                         .setCoreTerms(coreTerms).build();
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(extendedRecord);
        Assert.assertEquals("1.0", hdfsRecord.getVerbatimdepth());
        Assert.assertEquals("C1", hdfsRecord.getCollectioncode());
        Assert.assertEquals("I1", hdfsRecord.getInstitutioncode());
        Assert.assertEquals("CN1", hdfsRecord.getCatalognumber());
    }

    @Test
    public void multimediaConverterTest() {
        MultimediaRecord multimediaRecord = new MultimediaRecord();
        multimediaRecord.setId("1");
        Multimedia multimedia = new Multimedia();
        multimedia.setType(MediaType.StillImage.name());
        multimedia.setLicense(License.CC_BY_4_0.name());
        multimedia.setSource("image.jpg");
        multimediaRecord.setMultimediaItems(Collections.singletonList(multimedia));
        OccurrenceHdfsRecord hdfsRecord = OccurrenceHdfsRecordConverter.toOccurrenceHdfsRecord(multimediaRecord);
        Assert.assertTrue(hdfsRecord.getMediatype().contains(MediaType.StillImage.name()));
    }
}
