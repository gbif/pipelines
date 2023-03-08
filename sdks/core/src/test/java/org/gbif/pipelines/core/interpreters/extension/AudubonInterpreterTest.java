package org.gbif.pipelines.core.interpreters.extension;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.gbif.pipelines.io.avro.Audubon;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;
import org.junit.Assert;
import org.junit.Test;

public class AudubonInterpreterTest {

  @Test
  public void audubonTest() {

    // Expected
    String expected =
        "{\"id\": \"id\", \"created\": null, \"audubonItems\": [{\"creator\": \"Jose Padial\", "
            + "\"creatorUri\": null, \"providerLiteral\": \"CM\", \"provider\": null, \"metadataCreatorLiteral\": null, "
            + "\"metadataCreator\": null, \"metadataProviderLiteral\": null, \"metadataProvider\": null, \"rights\": "
            + "\"http://creativecommons.org/publicdomain/zero/1.0/\", "
            + "\"rightsUri\": \"http://creativecommons.org/publicdomain/zero/1.0/\", \"owner\": "
            + "\"Carnegie Museum of Natural History Herps Collection (CM:Herps)\", \"usageTerms\": "
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
    audubon.put(
        "http://rs.tdwg.org/ac/terms/associatedSpecimenReference", "urn:catalog:CM:Herps:156879");
    audubon.put("http://ns.adobe.com/xap/1.0/rights/UsageTerms", "CC0 1.0 (Public-domain)");
    audubon.put("http://purl.org/dc/terms/modified", "2017-08-15");
    audubon.put("http://purl.org/dc/terms/title", "AMBYSTOMA MACULATUM");
    audubon.put("http://rs.tdwg.org/ac/terms/serviceExpectation", "online");
    audubon.put(
        "http://rs.tdwg.org/ac/terms/accessURI",
        "https://api.idigbio.org/v2/media/414560e3c8514266b6d9e1888a792564");
    audubon.put(
        "http://ns.adobe.com/xap/1.0/rights/Owner",
        "Carnegie Museum of Natural History Herps Collection (CM:Herps)");
    audubon.put("http://ns.adobe.com/xap/1.0/CreateDate", "2010/12/10");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar.toString());
  }

  @Test
  public void wrongFormatTest() {

    // Expected
    String expected =
        "{\"id\": \"id\", \"created\": null, \"audubonItems\": [{\"creator\": null, \"creatorUri\": null, "
            + "\"providerLiteral\": null, \"provider\": null, \"metadataCreatorLiteral\": null, \"metadataCreator\": null, "
            + "\"metadataProviderLiteral\": null, \"metadataProvider\": null, \"rights\": \"CC0 4.0\", \"rightsUri\": \"CC0 4.0\", "
            + "\"owner\": \"Naturalis Biodiversity Center\", \"usageTerms\": null, \"webStatement\": null, \"licenseLogoUrl\": null, "
            + "\"credit\": null, \"attributionLogoUrl\": null, \"attributionLinkUrl\": null, \"fundingAttribution\": null, "
            + "\"source\": null, \"sourceUri\": null, \"description\": null, \"caption\": \"ZMA.AVES.11080\", \"language\": null, "
            + "\"languageUri\": null, \"physicalSetting\": null, \"cvTerm\": null, \"subjectCategoryVocabulary\": null, \"tag\": null, "
            + "\"locationShown\": null, \"worldRegion\": null, \"countryCode\": null, \"countryName\": null, \"provinceState\": null, "
            + "\"city\": null, \"sublocation\": null, \"identifier\": \"http://medialib.naturalis.nl/file/id/ZMA.AVES.11080/format/large\", "
            + "\"type\": \"MovingImage\", \"typeUri\": null, \"subtypeLiteral\": null, \"subtype\": null, \"title\": null, \"modified\": null, "
            + "\"metadataDate\": null, \"metadataLanguageLiteral\": null, \"metadataLanguage\": null, \"providerManagedId\": null, "
            + "\"rating\": null, \"commenterLiteral\": null, \"commenter\": null, \"comments\": null, \"reviewerLiteral\": null, "
            + "\"reviewer\": null, \"reviewerComments\": null, \"available\": null, \"hasServiceAccessPoint\": null, "
            + "\"idOfContainingCollection\": null, \"relatedResourceId\": null, \"providerId\": null, \"derivedFrom\": null, "
            + "\"associatedSpecimenReference\": null, \"associatedObservationReference\": null, \"locationCreated\": null, "
            + "\"digitizationDate\": null, \"captureDevice\": null, \"resourceCreationTechnique\": null, \"accessUri\": "
            + "\"http://medialib.naturalis.nl/file/id/ZMA.AVES.11080/format/large\", \"format\": \"video/mp4\", \"formatUri\": null, "
            + "\"variantLiteral\": null, \"variant\": \"ac:GoodQuality\", \"variantDescription\": null, \"furtherInformationUrl\": null, "
            + "\"licensingException\": null, \"serviceExpectation\": null, \"hashFunction\": null, \"hashValue\": null, "
            + "\"PixelXDimension\": null, \"PixelYDimension\": null, \"taxonCoverage\": null, \"scientificName\": null, "
            + "\"identificationQualifier\": null, \"vernacularName\": null, \"nameAccordingTo\": null, \"scientificNameId\": null, "
            + "\"otherScientificName\": null, \"identifiedBy\": null, \"dateIdentified\": null, \"taxonCount\": null, "
            + "\"subjectPart\": null, \"sex\": null, \"lifeStage\": null, \"subjectOrientation\": null, \"preparations\": null, "
            + "\"temporal\": null, \"createDate\": null, \"timeOfDay\": null}], \"issues\": {\"issueList\": []}}";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon1 = new HashMap<>(8);

    audubon1.put("http://purl.org/dc/terms/format", "video/mp4");
    audubon1.put(
        "http://purl.org/dc/terms/identifier",
        "http://medialib.naturalis.nl/file/id/ZMA.AVES.11080/format/large");
    audubon1.put("http://rs.tdwg.org/ac/terms/caption", "ZMA.AVES.11080");
    audubon1.put("http://rs.tdwg.org/ac/terms/variant", "ac:GoodQuality");
    audubon1.put("http://purl.org/dc/terms/type", "StillImage");
    audubon1.put(
        "http://rs.tdwg.org/ac/terms/accessURI",
        "http://medialib.naturalis.nl/file/id/ZMA.AVES.11080/format/large");
    audubon1.put("http://purl.org/dc/terms/rights", "CC0 4.0");
    audubon1.put("http://ns.adobe.com/xap/1.0/rights/Owner", "Naturalis Biodiversity Center");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

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
    audubon.put("http://ns.adobe.com/xap/1.0/CreateDate", "not_a_date");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar.toString());
  }

  @Test
  public void swappedValuesTest() {

    String expected =
        "{\"id\": \"id\", \"created\": null, \"audubonItems\": [{\"creator\": null, \"creatorUri\": \"Jerome Fischer\", "
            + "\"providerLiteral\": null, \"provider\": null, \"metadataCreatorLiteral\": null, \"metadataCreator\": null,"
            + " \"metadataProviderLiteral\": null, \"metadataProvider\": null, \"rights\": \"http://creativecommons.org/licenses/by-nc-sa/4.0/\", "
            + "\"rightsUri\": \"http://creativecommons.org/licenses/by-nc-sa/4.0/\", \"owner\": \"Jerome Fischer\", \"usageTerms\": null, "
            + "\"webStatement\": null, \"licenseLogoUrl\": null, \"credit\": null, \"attributionLogoUrl\": null, \"attributionLinkUrl\": null,"
            + " \"fundingAttribution\": null, \"source\": null, \"sourceUri\": null, \"description\": \"27 s\", \"caption\": null, \"language\": null, "
            + "\"languageUri\": null, \"physicalSetting\": null, \"cvTerm\": null, \"subjectCategoryVocabulary\": null, \"tag\": null, "
            + "\"locationShown\": null, \"worldRegion\": null, \"countryCode\": null, \"countryName\": null, \"provinceState\": null, "
            + "\"city\": null, \"sublocation\": null, \"identifier\": \"https://data.biodiversitydata.nl/xeno-canto/observation/XC449504\", "
            + "\"type\": \"Sound\", \"typeUri\": null, \"subtypeLiteral\": null, \"subtype\": null, \"title\": null, \"modified\": null, "
            + "\"metadataDate\": null, \"metadataLanguageLiteral\": null, \"metadataLanguage\": null, \"providerManagedId\": null, "
            + "\"rating\": null, \"commenterLiteral\": null, \"commenter\": null, \"comments\": null, \"reviewerLiteral\": null,"
            + " \"reviewer\": null, \"reviewerComments\": null, \"available\": null, \"hasServiceAccessPoint\": null, \"idOfContainingCollection\": null, "
            + "\"relatedResourceId\": null, \"providerId\": null, \"derivedFrom\": null, \"associatedSpecimenReference\": null, "
            + "\"associatedObservationReference\": null, \"locationCreated\": null, \"digitizationDate\": null, \"captureDevice\": null, "
            + "\"resourceCreationTechnique\": \"bitrate: 320000 bps; bitrate mode: cbr; audio sampling rate: 44100 Hz; number of channels: 2; lossy\", "
            + "\"accessUri\": \"https://www.xeno-canto.org/sounds/uploaded/JPBSNBUUEF/XC449504-Rufous-winged%20Antwren%2C%20song%2C%203.1..mp3\", "
            + "\"format\": \"audio/mpeg\", \"formatUri\": null, \"variantLiteral\": \"ac:BestQuality\", \"variant\": null, \"variantDescription\": null, "
            + "\"furtherInformationUrl\": null, \"licensingException\": null, \"serviceExpectation\": null, \"hashFunction\": null, \"hashValue\": null, "
            + "\"PixelXDimension\": null, \"PixelYDimension\": null, \"taxonCoverage\": null, \"scientificName\": null, \"identificationQualifier\": null, "
            + "\"vernacularName\": null, \"nameAccordingTo\": null, \"scientificNameId\": null, \"otherScientificName\": null, \"identifiedBy\": null, "
            + "\"dateIdentified\": null, \"taxonCount\": null, \"subjectPart\": null, \"sex\": null, \"lifeStage\": null, \"subjectOrientation\": null, "
            + "\"preparations\": null, \"temporal\": null, \"createDate\": null, \"timeOfDay\": null}, {\"creator\": null, \"creatorUri\": "
            + "\"Stichting Xeno-canto voor Natuurgeluiden\", \"providerLiteral\": null, \"provider\": null, \"metadataCreatorLiteral\": null, "
            + "\"metadataCreator\": null, \"metadataProviderLiteral\": null, \"metadataProvider\": null, \"rights\": \"http://creativecommons.org/licenses/by-nc-sa/4.0/\", "
            + "\"rightsUri\": \"http://creativecommons.org/licenses/by-nc-sa/4.0/\", \"owner\": \"Stichting Xeno-canto voor Natuurgeluiden\", "
            + "\"usageTerms\": null, \"webStatement\": null, \"licenseLogoUrl\": null, \"credit\": null, \"attributionLogoUrl\": null, "
            + "\"attributionLinkUrl\": null, \"fundingAttribution\": null, \"source\": null, \"sourceUri\": null, \"description\": null, "
            + "\"caption\": \"Sonogram of the first ten seconds of the sound recording\", \"language\": null, \"languageUri\": null, "
            + "\"physicalSetting\": null, \"cvTerm\": null, \"subjectCategoryVocabulary\": null, \"tag\": null, \"locationShown\": null, "
            + "\"worldRegion\": null, \"countryCode\": null, \"countryName\": null, \"provinceState\": null, \"city\": null, \"sublocation\": null, "
            + "\"identifier\": \"https://data.biodiversitydata.nl/xeno-canto/observation/XC449504\", \"type\": \"StillImage\", \"typeUri\": null, "
            + "\"subtypeLiteral\": null, \"subtype\": null, \"title\": null, \"modified\": null, \"metadataDate\": null, "
            + "\"metadataLanguageLiteral\": null, \"metadataLanguage\": null, \"providerManagedId\": null, \"rating\": null, \"commenterLiteral\": null, "
            + "\"commenter\": null, \"comments\": null, \"reviewerLiteral\": null, \"reviewer\": null, \"reviewerComments\": null, \"available\": null, "
            + "\"hasServiceAccessPoint\": null, \"idOfContainingCollection\": null, \"relatedResourceId\": null, \"providerId\": null,"
            + " \"derivedFrom\": null, \"associatedSpecimenReference\": null, \"associatedObservationReference\": null, \"locationCreated\": null, "
            + "\"digitizationDate\": null, \"captureDevice\": null, \"resourceCreationTechnique\": null, \"accessUri\": "
            + "\"https://www.xeno-canto.org/sounds/uploaded/JPBSNBUUEF/ffts/XC449504-large.png\", \"format\": \"image/png\", \"formatUri\": null, "
            + "\"variantLiteral\": \"ac:MediumQuality\", \"variant\": null, \"variantDescription\": null, \"furtherInformationUrl\": null, "
            + "\"licensingException\": null, \"serviceExpectation\": null, \"hashFunction\": null, \"hashValue\": null, \"PixelXDimension\": null, "
            + "\"PixelYDimension\": null, \"taxonCoverage\": null, \"scientificName\": null, \"identificationQualifier\": null, \"vernacularName\": null, "
            + "\"nameAccordingTo\": null, \"scientificNameId\": null, \"otherScientificName\": null, \"identifiedBy\": null, \"dateIdentified\": null, "
            + "\"taxonCount\": null, \"subjectPart\": null, \"sex\": null, \"lifeStage\": null, \"subjectOrientation\": null, \"preparations\": null,"
            + " \"temporal\": null, \"createDate\": null, \"timeOfDay\": null}], \"issues\": {\"issueList\": []}}";

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(2);
    Map<String, String> audubon1 = new HashMap<>(10);
    audubon1.put("http://purl.org/dc/terms/format", "audio/mp3");
    audubon1.put("http://purl.org/dc/terms/creator", "Jerome Fischer");
    audubon1.put(
        "http://purl.org/dc/terms/identifier",
        "https://data.biodiversitydata.nl/xeno-canto/observation/XC449504");
    audubon1.put("http://purl.org/dc/terms/type", "Sound");
    audubon1.put(
        "http://rs.tdwg.org/ac/terms/resourceCreationTechnique",
        "bitrate: 320000 bps; bitrate mode: cbr; audio sampling rate: 44100 Hz; number of channels: 2; lossy");
    audubon1.put("http://purl.org/dc/terms/description", "27 s");
    audubon1.put(
        "http://rs.tdwg.org/ac/terms/accessURI",
        "https://www.xeno-canto.org/sounds/uploaded/JPBSNBUUEF/XC449504-Rufous-winged%20Antwren%2C%20song%2C%203.1..mp3");
    audubon1.put("http://rs.tdwg.org/ac/terms/variantLiteral", "ac:BestQuality");
    audubon1.put("http://ns.adobe.com/xap/1.0/rights/Owner", "Jerome Fischer");
    audubon1.put("http://purl.org/dc/terms/rights", "CC BY-NC-SA 4.0");

    Map<String, String> audubon2 = new HashMap<>(9);
    audubon2.put("http://purl.org/dc/terms/format", "image/png");
    audubon2.put("http://purl.org/dc/terms/creator", "Stichting Xeno-canto voor Natuurgeluiden");
    audubon2.put(
        "http://rs.tdwg.org/ac/terms/caption",
        "Sonogram of the first ten seconds of the sound recording");
    audubon2.put(
        "http://purl.org/dc/terms/identifier",
        "https://data.biodiversitydata.nl/xeno-canto/observation/XC449504");
    audubon2.put("http://purl.org/dc/terms/type", "StillImage");
    audubon2.put(
        "http://rs.tdwg.org/ac/terms/accessURI",
        "https://www.xeno-canto.org/sounds/uploaded/JPBSNBUUEF/ffts/XC449504-large.png");
    audubon2.put("http://rs.tdwg.org/ac/terms/variantLiteral", "ac:MediumQuality");
    audubon2.put(
        "http://ns.adobe.com/xap/1.0/rights/Owner", "Stichting Xeno-canto voor Natuurgeluiden");
    audubon2.put("http://purl.org/dc/terms/rights", "CC BY-NC-SA 4.0");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Arrays.asList(audubon1, audubon2));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar.toString());
  }

  @Test
  public void licensePriorityTest() {

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon1 = new HashMap<>(2);
    audubon1.put(
        "http://purl.org/dc/elements/1.1/rights",
        "https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode");
    audubon1.put(
        "http://purl.org/dc/terms/rights",
        "http://creativecommons.org/licences/by-nc-sa/3.0/legalcode");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    AudubonRecord expected =
        AudubonRecord.newBuilder()
            .setId("id")
            .setAudubonItems(
                Collections.singletonList(
                    Audubon.newBuilder()
                        .setRights("http://creativecommons.org/licenses/by-nc-sa/3.0/")
                        .setRightsUri("http://creativecommons.org/licenses/by-nc-sa/3.0/")
                        .build()))
            .build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar);
  }

  @Test
  public void licenseTest() {

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon1 = new HashMap<>(1);
    audubon1.put(
        "http://purl.org/dc/terms/rights",
        "http://creativecommons.org/licences/by-nc-sa/3.0/legalcode");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    AudubonRecord expected =
        AudubonRecord.newBuilder()
            .setId("id")
            .setAudubonItems(
                Collections.singletonList(
                    Audubon.newBuilder()
                        .setRights("http://creativecommons.org/licenses/by-nc-sa/3.0/")
                        .setRightsUri("http://creativecommons.org/licenses/by-nc-sa/3.0/")
                        .build()))
            .build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar);
  }

  @Test
  public void licenseUriTest() {

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon1 = new HashMap<>(1);
    audubon1.put(
        "http://purl.org/dc/elements/1.1/rights",
        "https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    AudubonRecord expected =
        AudubonRecord.newBuilder()
            .setId("id")
            .setAudubonItems(
                Collections.singletonList(
                    Audubon.newBuilder()
                        .setRights("http://creativecommons.org/licenses/by-nc-sa/4.0/")
                        .setRightsUri("http://creativecommons.org/licenses/by-nc-sa/4.0/")
                        .build()))
            .build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar);
  }

  @Test
  public void accessUriTest() {

    // State
    Map<String, List<Map<String, String>>> ext = new HashMap<>(1);
    Map<String, String> audubon1 = new HashMap<>(4);
    audubon1.put(
        "http://purl.org/dc/elements/1.1/rights",
        "https://creativecommons.org/licenses/by-nc-sa/4.0/legalcode");
    audubon1.put(
        "http://purl.org/dc/terms/identifier",
        "aoeu-aoeu-aoeu-aoeu");
    audubon1.put(
        "http://rs.tdwg.org/ac/terms/accessURI",
        "https://quod.lib.umich.edu/cgi/i/image/api/image/herb00ic:1559372:MICH-V-1559372/full/res:0/0/native.jpg");
    audubon1.put("http://ns.adobe.com/xap/1.0/MetadataDate", "2019-07-12 06:30:57.0");

    ext.put("http://rs.tdwg.org/ac/terms/Multimedia", Collections.singletonList(audubon1));

    ExtendedRecord er = ExtendedRecord.newBuilder().setId("id").setExtensions(ext).build();

    AudubonRecord ar = AudubonRecord.newBuilder().setId("id").build();

    AudubonRecord expected =
        AudubonRecord.newBuilder()
            .setId("id")
            .setAudubonItems(
                Collections.singletonList(
                    Audubon.newBuilder()
                        .setRights("http://creativecommons.org/licenses/by-nc-sa/4.0/")
                        .setRightsUri("http://creativecommons.org/licenses/by-nc-sa/4.0/")
                        .setAccessUri(
                            "https://quod.lib.umich.edu/cgi/i/image/api/image/herb00ic:1559372:MICH-V-1559372/full/res:0/0/native.jpg")
                        .setIdentifier(
                            "aoeu-aoeu-aoeu-aoeu")
                        .setFormat("image/jpeg")
                        .setType("StillImage")
                        .setMetadataDate("2019-07-12 06:30:57.0")
                        .build()))
            .build();

    // When
    AudubonInterpreter.builder().create().interpret(er, ar);

    // Should
    Assert.assertEquals(expected, ar);
  }
}
