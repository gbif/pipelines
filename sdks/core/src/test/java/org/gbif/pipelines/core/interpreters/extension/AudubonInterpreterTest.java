package org.gbif.pipelines.core.interpreters.extension;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import org.junit.Assert;
import org.junit.Test;

public class AudubonInterpreterTest {

  @Test
  public void audubonTest() {

    // Expected
    String expected = "{\"id\": \"id\", \"created\": null, \"audubonItems\": [{\"creator\": \"Jose Padial\", "
        + "\"creatorUri\": null, \"providerLiteral\": \"CM\", \"provider\": null, \"metadataCreatorLiteral\": null, "
        + "\"metadataCreator\": null, \"metadataProviderLiteral\": null, \"metadataProvider\": null, \"rights\": \"CC0\", "
        + "\"rightsUri\": null, \"owner\": \"Carnegie Museum of Natural History Herps Collection (CM:Herps)\", \"usageTerms\": "
        + "\"CC0 1.0 (Public-domain)\", \"webStatement\": null, \"licenseLogoUrl\": null, \"credit\": null, "
        + "\"attributionLogoUrl\": null, \"attributionLinkUrl\": null, \"fundingAttribution\": null, \"source\": null, "
        + "\"sourceUri\": null, \"description\": \"Photo taken in 2013\", \"caption\": null, \"language\": null, "
        + "\"languageUri\": null, \"physicalSetting\": null, \"cvTerm\": null, \"subjectCategoryVocabulary\": null, "
        + "\"tag\": null, \"locationShown\": null, \"worldRegion\": null, \"countryCode\": null, \"countryName\": null, "
        + "\"provinceState\": null, \"city\": null, \"sublocation\": null, \"identifier\": \"1b384fd8-8559-42ba-980f-22661a4b5f75\", "
        + "\"type\": \"StillImage\", \"typeUri\": null, \"subtypeLiteral\": \"Photograph\", \"subtype\": null, \"title\": "
        + "\"AMBYSTOMA MACULATUM\", \"modified\": \"2017-08-15\", \"metadataDate\": null, \"metadataLanguageLiteral\": null, "
        + "\"metadataLanguage\": null, \"providerManagedId\": null, \"rating\": null, \"commenterLiteral\": null, "
        + "\"commenter\": null, \"comments\": null, \"reviewerLiteral\": null, \"reviewer\": null, \"reviewerComments\": null, "
        + "\"available\": null, \"hasServiceAccessPoint\": null, \"idOfContainingCollection\": null, \"relatedResourceId\": null, "
        + "\"providerId\": null, \"derivedFrom\": null, \"associatedSpecimenReference\": \"urn:catalog:CM:Herps:156879\", "
        + "\"associatedObservationReference\": null, \"locationCreated\": null, \"digitizationDate\": null, \"captureDevice\": null, "
        + "\"resourceCreationTechnique\": null, \"accessUri\": \"https://api.idigbio.org/v2/media/414560e3c8514266b6d9e1888a792564\", "
        + "\"format\": \"image/jpeg\", \"formatUri\": null, \"variantLiteral\": null, \"variant\": null, \"variantDescription\": null, "
        + "\"furtherInformationUrl\": null, \"licensingException\": null, \"serviceExpectation\": \"online\", \"hashFunction\": null, "
        + "\"hashValue\": null, \"PixelXDimension\": null, \"PixelYDimension\": null, \"taxonCoverage\": null, \"scientificName\": null, "
        + "\"identificationQualifier\": null, \"vernacularName\": null, \"nameAccordingTo\": null, \"scientificNameId\": null, "
        + "\"otherScientificName\": null, \"identifiedBy\": null, \"dateIdentified\": null, \"taxonCount\": null, \"subjectPart\": null, "
        + "\"sex\": null, \"lifeStage\": null, \"subjectOrientation\": null, \"preparations\": null, \"temporal\": null, "
        + "\"createDate\": \"2010-12-10\", \"timeOfDay\": null}], \"issues\": {\"issueList\": []}}";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon = new HashMap<>(16);
    audubon.put("http://purl.org/dc/terms/identifier", "1b384fd8-8559-42ba-980f-22661a4b5f75");
    audubon.put("http://purl.org/dc/elements/1.1/rights", "CC0");
    audubon.put("http://purl.org/dc/elements/1.1/format", "image/jpeg");
    audubon.put("http://purl.org/dc/elements/1.1/type", "StillImage");
    audubon.put("http://purl.org/dc/terms/description", "Photo taken in 2013");
    audubon.put("http://rs.tdwg.org/ac/terms/subtypeLiteral", "Photograph");
    audubon.put("http://purl.org/dc/elements/1.1/creator", "Jose Padial");
    audubon.put("http://rs.tdwg.org/ac/terms/providerLiteral", "CM");
    audubon.put("http://rs.tdwg.org/ac/terms/associatedSpecimenReference", "urn:catalog:CM:Herps:156879");
    audubon.put("http://ns.adobe.com/xap/1.0/rights/UsageTerms", "CC0 1.0 (Public-domain)");
    audubon.put("http://purl.org/dc/terms/modified", "2017-08-15");
    audubon.put("http://purl.org/dc/terms/title", "AMBYSTOMA MACULATUM");
    audubon.put("http://rs.tdwg.org/ac/terms/serviceExpectation", "online");
    audubon.put("http://rs.tdwg.org/ac/terms/accessURI",
        "https://api.idigbio.org/v2/media/414560e3c8514266b6d9e1888a792564");
    audubon.put("http://ns.adobe.com/xap/1.0/rights/Owner",
        "Carnegie Museum of Natural History Herps Collection (CM:Herps)");
    audubon.put("http://ns.adobe.com/xap/1.0/CreateDate", "2010/12/10");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    // When
    AudubonInterpreter.interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar.toString());
  }

  @Test
  public void dateIssueTest() {

    // Expected
    String expected =
        "{\"id\": \"id\", \"created\": null, \"audubonItems\": [{\"creator\": null, \"creatorUri\": null, \"providerLiteral\": "
            + "null, \"provider\": null, \"metadataCreatorLiteral\": null, \"metadataCreator\": null, \"metadataProviderLiteral\": null, "
            + "\"metadataProvider\": null, \"rights\": null, \"rightsUri\": null, \"owner\": null, \"usageTerms\": null, "
            + "\"webStatement\": null, \"licenseLogoUrl\": null, \"credit\": null, \"attributionLogoUrl\": null, "
            + "\"attributionLinkUrl\": null, \"fundingAttribution\": null, \"source\": null, \"sourceUri\": null, \"description\": null, "
            + "\"caption\": null, \"language\": null, \"languageUri\": null, \"physicalSetting\": null, \"cvTerm\": null, "
            + "\"subjectCategoryVocabulary\": null, \"tag\": null, \"locationShown\": null, \"worldRegion\": null, \"countryCode\": null, "
            + "\"countryName\": null, \"provinceState\": null, \"city\": null, \"sublocation\": null, \"identifier\": null, \"type\": "
            + "\"StillImage\", \"typeUri\": null, \"subtypeLiteral\": null, \"subtype\": null, \"title\": null, \"modified\": null, "
            + "\"metadataDate\": null, \"metadataLanguageLiteral\": null, \"metadataLanguage\": null, \"providerManagedId\": null, "
            + "\"rating\": null, \"commenterLiteral\": null, \"commenter\": null, \"comments\": null, \"reviewerLiteral\": null, "
            + "\"reviewer\": null, \"reviewerComments\": null, \"available\": null, \"hasServiceAccessPoint\": null, "
            + "\"idOfContainingCollection\": null, \"relatedResourceId\": null, \"providerId\": null, \"derivedFrom\": null, "
            + "\"associatedSpecimenReference\": null, \"associatedObservationReference\": null, \"locationCreated\": null, "
            + "\"digitizationDate\": null, \"captureDevice\": null, \"resourceCreationTechnique\": null, \"accessUri\": null, "
            + "\"format\": null, \"formatUri\": null, \"variantLiteral\": null, \"variant\": null, \"variantDescription\": null, "
            + "\"furtherInformationUrl\": null, \"licensingException\": null, \"serviceExpectation\": null, \"hashFunction\": null, "
            + "\"hashValue\": null, \"PixelXDimension\": null, \"PixelYDimension\": null, \"taxonCoverage\": null, \"scientificName\": null, "
            + "\"identificationQualifier\": null, \"vernacularName\": null, \"nameAccordingTo\": null, \"scientificNameId\": null, "
            + "\"otherScientificName\": null, \"identifiedBy\": null, \"dateIdentified\": null, \"taxonCount\": null, \"subjectPart\": null, "
            + "\"sex\": null, \"lifeStage\": null, \"subjectOrientation\": null, \"preparations\": null, \"temporal\": null, "
            + "\"createDate\": null, \"timeOfDay\": null}], \"issues\": {\"issueList\": [\"MULTIMEDIA_DATE_INVALID\"]}}";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon = new HashMap<>(2);
    audubon.put("http://purl.org/dc/elements/1.1/type", "image");
    audubon.put("http://ns.adobe.com/xap/1.0/CreateDate", "2020/12/10");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    // When
    AudubonInterpreter.interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar.toString());
  }

}
