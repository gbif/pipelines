package org.gbif.pipelines.core.interpreters.extension;

import static org.gbif.api.vocabulary.OccurrenceIssue.MULTIMEDIA_DATE_INVALID;

import com.google.common.base.Strings;
import java.net.URI;
import java.time.temporal.TemporalAccessor;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiPredicate;
import lombok.Builder;
import org.gbif.api.vocabulary.Extension;
import org.gbif.api.vocabulary.MediaType;
import org.gbif.api.vocabulary.OccurrenceIssue;
import org.gbif.common.parsers.LicenseUriParser;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.common.parsers.core.OccurrenceParseResult;
import org.gbif.common.parsers.core.ParseResult;
import org.gbif.common.parsers.date.DateComponentOrdering;
import org.gbif.dwc.terms.*;
import org.gbif.pipelines.core.functions.SerializableFunction;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.interpreters.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.core.parsers.temporal.TemporalParser;
import org.gbif.pipelines.io.avro.Audubon;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

/**
 * Interpreter for the Audubon extension, Interprets form {@link ExtendedRecord} to {@link
 * AudubonRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/ac/audubon.xml</a>
 */
public class AudubonInterpreter {

  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();
  private static final LicenseUriParser LICENSE_URI_PARSER = LicenseUriParser.getInstance();

  private static final String IPTC = "http://iptc.org/std/Iptc4xmpExt/2008-02-29/";

  private final TargetHandler<Audubon> handler =
      ExtensionInterpretation.extension(Extension.AUDUBON)
          .to(Audubon::new)
          .map(DcElement.creator, Audubon::setCreator)
          .map(DcTerm.creator, Audubon::setCreatorUri)
          .map(AcTerm.providerLiteral, Audubon::setProviderLiteral)
          .map(AcTerm.provider, Audubon::setProvider)
          .map(AcTerm.metadataCreatorLiteral, Audubon::setMetadataCreatorLiteral)
          .map(AcTerm.metadataCreator, Audubon::setMetadataCreator)
          .map(AcTerm.metadataProviderLiteral, Audubon::setMetadataProviderLiteral)
          .map(AcTerm.metadataProvider, Audubon::setMetadataProvider)
          .map(DcElement.rights, Audubon::setRights)
          .map(DcTerm.rights, Audubon::setRightsUri)
          .map(XmpRightsTerm.Owner, Audubon::setOwner)
          .map(XmpRightsTerm.UsageTerms, Audubon::setUsageTerms)
          .map(XmpRightsTerm.WebStatement, Audubon::setWebStatement)
          .map(AcTerm.licenseLogoURL, Audubon::setLicenseLogoUrl)
          .map(AcTerm.attributionLogoURL, Audubon::setAttributionLogoUrl)
          .map(AcTerm.fundingAttribution, Audubon::setFundingAttribution)
          .map(DcElement.source, Audubon::setSource)
          .map(DcTerm.source, Audubon::setSourceUri)
          .map(DcTerm.description, Audubon::setDescription)
          .map(AcTerm.caption, Audubon::setCaption)
          .map(DcElement.language, Audubon::setLanguage)
          .map(DcTerm.language, Audubon::setLanguageUri)
          .map(AcTerm.physicalSetting, Audubon::setPhysicalSetting)
          .map(AcTerm.subjectCategoryVocabulary, Audubon::setSubjectCategoryVocabulary)
          .map(AcTerm.tag, Audubon::setTag)
          .map(DcTerm.identifier, Audubon::setIdentifier)
          .map(AcTerm.subtypeLiteral, Audubon::setSubtypeLiteral)
          .map(AcTerm.subtype, Audubon::setSubtype)
          .map(DcTerm.title, Audubon::setTitle)
          .map(DcTerm.modified, Audubon::setModified)
          .map(XmpTerm.MetadataDate, Audubon::setMetadataDate)
          .map(AcTerm.metadataLanguageLiteral, Audubon::setMetadataLanguageLiteral)
          .map(AcTerm.metadataLanguage, Audubon::setMetadataLanguage)
          .map(AcTerm.providerManagedID, Audubon::setProviderManagedId)
          .map(XmpTerm.Rating, Audubon::setRating)
          .map(AcTerm.commenterLiteral, Audubon::setCommenterLiteral)
          .map(AcTerm.commenter, Audubon::setCommenter)
          .map(AcTerm.comments, Audubon::setComments)
          .map(AcTerm.reviewerLiteral, Audubon::setReviewerLiteral)
          .map(AcTerm.reviewer, Audubon::setReviewer)
          .map(AcTerm.reviewerComments, Audubon::setReviewerComments)
          .map(DcTerm.available, Audubon::setAvailable)
          .map(AcTerm.hasServiceAccessPoint, Audubon::setHasServiceAccessPoint)
          .map(AcTerm.providerID, Audubon::setProviderId)
          .map(AcTerm.derivedFrom, Audubon::setDerivedFrom)
          .map(AcTerm.associatedSpecimenReference, Audubon::setAssociatedSpecimenReference)
          .map(AcTerm.associatedObservationReference, Audubon::setAssociatedObservationReference)
          .map(AcTerm.digitizationDate, Audubon::setDigitizationDate)
          .map(AcTerm.captureDevice, Audubon::setCaptureDevice)
          .map(AcTerm.resourceCreationTechnique, Audubon::setResourceCreationTechnique)
          .map(DcElement.format, Audubon::setFormat)
          .map(AcTerm.variantLiteral, Audubon::setVariantLiteral)
          .map(AcTerm.variant, Audubon::setVariant)
          .map(AcTerm.variantDescription, Audubon::setVariantDescription)
          .map(AcTerm.licensingException, Audubon::setLicensingException)
          .map(AcTerm.serviceExpectation, Audubon::setServiceExpectation)
          .map(AcTerm.hashFunction, Audubon::setHashFunction)
          .map(AcTerm.hashValue, Audubon::setHashValue)
          .map(AcTerm.taxonCoverage, Audubon::setTaxonCoverage)
          .map(DwcTerm.scientificName, Audubon::setScientificName)
          .map(DwcTerm.identificationQualifier, Audubon::setIdentificationQualifier)
          .map(DwcTerm.vernacularName, Audubon::setVernacularName)
          .map(DwcTerm.nameAccordingTo, Audubon::setNameAccordingTo)
          .map(DwcTerm.scientificNameID, Audubon::setScientificNameId)
          .map(AcTerm.otherScientificName, Audubon::setOtherScientificName)
          .map(DwcTerm.identifiedBy, Audubon::setIdentifiedBy)
          .map(DwcTerm.dateIdentified, Audubon::setDateIdentified)
          .map(AcTerm.taxonCount, Audubon::setTaxonCount)
          .map(AcTerm.subjectPart, Audubon::setSubjectPart)
          .map(DwcTerm.sex, Audubon::setSex)
          .map(DwcTerm.lifeStage, Audubon::setLifeStage)
          .map(AcTerm.subjectOrientation, Audubon::setSubjectOrientation)
          .map(DwcTerm.preparations, Audubon::setPreparations)
          .map(DcTerm.temporal, Audubon::setTemporal)
          .map(AcTerm.timeOfDay, Audubon::setTimeOfDay)
          .map(AcTerm.IDofContainingCollection, Audubon::setIdOfContainingCollection)
          .map(IPTC + "LocationCreated", Audubon::setLocationCreated)
          .map(IPTC + "CVterm", Audubon::setCvTerm)
          .map(IPTC + "LocationShown", Audubon::setLocationShown)
          .map(IPTC + "WorldRegion", Audubon::setWorldRegion)
          .map(IPTC + "CountryCode", Audubon::setCountryCode)
          .map(IPTC + "CountryName", Audubon::setCountryName)
          .map(IPTC + "ProvinceState", Audubon::setProvinceState)
          .map(IPTC + "City", Audubon::setCity)
          .map(IPTC + "Sublocation", Audubon::setSublocation)
          .map("http://rs.tdwg.org/ac/terms/relatedResourceID", Audubon::setRelatedResourceId)
          .map("http://ns.adobe.com/exif/1.0/PixelXDimension", Audubon::setPixelXDimension)
          .map("http://ns.adobe.com/exif/1.0/PixelYDimension", Audubon::setPixelYDimension)
          .map("http://ns.adobe.com/photoshop/1.0/Credit", Audubon::setCredit)
          .map(DcTerm.format, AudubonInterpreter::parseAndSetFormat)
          .map(AcTerm.furtherInformationURL, AudubonInterpreter::parseAndSetFurtherInformationUrl)
          .map(AcTerm.attributionLinkURL, AudubonInterpreter::parseAndSetAttributionLinkUrl)
          .mapOne(AcTerm.accessURI, AudubonInterpreter::parseAndSetAccessUri)
          .mapOne(XmpTerm.CreateDate, this::parseAndSetCreatedDate)
          .map(DcTerm.type, AudubonInterpreter::parseAndSetTypeUri)
          .map(DcElement.type, AudubonInterpreter::parseAndSetType)
          .postMap(AudubonInterpreter::parseAndSetRightsAndRightsUri)
          .postMap(AudubonInterpreter::parseAndSetMissingTypeOrFormat);

  private final TemporalParser temporalParser;
  private final SerializableFunction<String, String> preprocessDateFn;

  @Builder(buildMethodName = "create")
  private AudubonInterpreter(
      List<DateComponentOrdering> orderings,
      SerializableFunction<String, String> preprocessDateFn) {
    this.temporalParser = TemporalParser.create(orderings);
    this.preprocessDateFn = preprocessDateFn;
  }

  /**
   * Interprets audubon of a {@link ExtendedRecord} and populates a {@link AudubonRecord} with the
   * interpreted values.
   */
  public void interpret(ExtendedRecord er, AudubonRecord ar) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(ar);

    Result<Audubon> result = handler.convert(er);

    ar.setAudubonItems(result.getList());
    ar.getIssues().setIssueList(result.getIssuesAsList());
  }

  /** Parser for "http://rs.tdwg.org/ac/terms/accessURI" term value */
  private static String parseAndSetAccessUri(Audubon a, String v) {
    URI uri = UrlParser.parse(v);
    Optional<URI> uriOpt = Optional.ofNullable(uri);
    if (uriOpt.isPresent()) {
      Optional<String> opt = uriOpt.map(URI::toString);
      if (opt.isPresent()) {
        opt.ifPresent(a::setAccessUri);
      } else {
        return OccurrenceIssue.MULTIMEDIA_URI_INVALID.name();
      }
    } else {
      return OccurrenceIssue.MULTIMEDIA_URI_INVALID.name();
    }
    return "";
  }

  /** Parser for "http://rs.tdwg.org/ac/terms/furtherInformationURL" term value */
  private static void parseAndSetFurtherInformationUrl(Audubon a, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(a::setFurtherInformationUrl);
  }

  /** Parser for "http://rs.tdwg.org/ac/terms/attributionLinkURL" term value */
  private static void parseAndSetAttributionLinkUrl(Audubon a, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(a::setAttributionLinkUrl);
  }

  /** Parser for "http://purl.org/dc/terms/format" term value */
  private static void parseAndSetFormat(Audubon a, String v) {
    String mimeType = MEDIA_PARSER.parseMimeType(v);

    a.setFormat(mimeType);
  }

  /** Parser for "http://purl.org/dc/elements/1.1/type" term value */
  private static void parseAndSetType(Audubon a, String v) {
    String v1 = Optional.ofNullable(v).orElse("");
    String format = Optional.ofNullable(a.getFormat()).orElse("");
    BiPredicate<String, MediaType> prFn =
        (s, mt) ->
            format.startsWith(s)
                || v1.toLowerCase().startsWith(s)
                || v1.toLowerCase().startsWith(mt.name().toLowerCase());

    if (prFn.test("video", MediaType.MovingImage)) {
      a.setType(MediaType.MovingImage.name());
    } else if (prFn.test("audio", MediaType.Sound)) {
      a.setType(MediaType.Sound.name());
    } else if (prFn.test("image", MediaType.StillImage)) {
      a.setType(MediaType.StillImage.name());
    } else if (prFn.test("application/json+ld", MediaType.InteractiveResource)) {
      a.setType(MediaType.InteractiveResource.name());
    }
  }

  /** Parser for "http://purl.org/dc/terms/type" term value */
  private static void parseAndSetTypeUri(Audubon a, String v) {
    parseAndSetType(a, v);
  }

  /** Returns ENUM instead of url string */
  private static void parseAndSetRightsAndRightsUri(Audubon a) {
    String rightsUri = Strings.isNullOrEmpty(a.getRightsUri()) ? a.getRights() : a.getRightsUri();
    if (Objects.nonNull(rightsUri)) {
      ParseResult<URI> parsed = LICENSE_URI_PARSER.parse(rightsUri);
      String licenseUri = parsed.isSuccessful() ? parsed.getPayload().toString() : rightsUri;
      a.setRights(licenseUri);
      a.setRightsUri(licenseUri);
    }
  }

  /**
   * Attempts deriving the format from the accessURI (e.g. ending with .jpg)
   * and the type from the format, if either is null.
   */
  private static void parseAndSetMissingTypeOrFormat(Audubon a) {
    if (a.getFormat() == null && !Strings.isNullOrEmpty(a.getAccessUri())) {
      try {
        parseAndSetFormat(a, MEDIA_PARSER.parseMimeType(URI.create(a.getAccessUri())));
      } catch (IllegalArgumentException ex) {}
    }

    if (a.getType() == null && a.getFormat() != null) {
      parseAndSetType(a, a.getFormat());
    }
  }

  /** Parser for "http://ns.adobe.com/xap/1.0/CreateDate" term value */
  private String parseAndSetCreatedDate(Audubon a, String v) {
    String normalizedDate = Optional.ofNullable(preprocessDateFn).map(x -> x.apply(v)).orElse(v);
    OccurrenceParseResult<TemporalAccessor> result =
        temporalParser.parseRecordedDate(normalizedDate);
    if (result.isSuccessful()) {
      a.setCreateDate(result.getPayload().toString());
      return "";
    } else {
      return MULTIMEDIA_DATE_INVALID.name();
    }
  }
}
