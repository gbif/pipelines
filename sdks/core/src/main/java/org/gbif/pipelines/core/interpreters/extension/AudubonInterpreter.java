package org.gbif.pipelines.core.interpreters.extension;

import java.net.URI;
import java.util.Objects;
import java.util.Optional;

import org.gbif.api.vocabulary.Extension;
import org.gbif.common.parsers.MediaParser;
import org.gbif.common.parsers.UrlParser;
import org.gbif.dwc.terms.AcTerm;
import org.gbif.dwc.terms.DcElement;
import org.gbif.dwc.terms.DcTerm;
import org.gbif.dwc.terms.DwcTerm;
import org.gbif.dwc.terms.XmpRightsTerm;
import org.gbif.dwc.terms.XmpTerm;
import org.gbif.pipelines.core.ExtensionInterpretation;
import org.gbif.pipelines.core.ExtensionInterpretation.Result;
import org.gbif.pipelines.core.ExtensionInterpretation.TargetHandler;
import org.gbif.pipelines.io.avro.Audubon;
import org.gbif.pipelines.io.avro.AudubonRecord;
import org.gbif.pipelines.io.avro.ExtendedRecord;

import com.google.common.base.Strings;

/**
 * Interpreter for the Audubon extension, Interprets form {@link ExtendedRecord} to {@link AudubonRecord}.
 *
 * @see <a href="http://rs.gbif.org/extension/ac/audubon.xml</a>
 */
public class AudubonInterpreter {

  private static final MediaParser MEDIA_PARSER = MediaParser.getInstance();

  private static final String IPTC = "http://iptc.org/std/Iptc4xmpExt/2008-02-29/";

  private static final TargetHandler<Audubon> HANDLER =
      ExtensionInterpretation.extenstion(Extension.AUDUBON)
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
          .map(DcElement.type, Audubon::setType)
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
          .map(XmpTerm.CreateDate, Audubon::setCreateDate)
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
          .map(DcTerm.type, Audubon::setTypeUri)
          .map(AcTerm.furtherInformationURL, AudubonInterpreter::parseAndsetFurtherInformationUrl)
          .map(AcTerm.attributionLinkURL, AudubonInterpreter::parseAndsetAttributionLinkUrl)
          .map(AcTerm.accessURI, AudubonInterpreter::parseAndsetAccessUri)
          .map(DcTerm.format, AudubonInterpreter::parseAndSetFormat);

  private AudubonInterpreter() {}

  /**
   * Interprets audubons of a {@link ExtendedRecord} and populates a {@link AudubonRecord}
   * with the interpreted values.
   */
  public static void interpret(ExtendedRecord er, AudubonRecord ar) {
    Objects.requireNonNull(er);
    Objects.requireNonNull(ar);

    Result<Audubon> result = HANDLER.convert(er);

    ar.setAudubonItems(result.getList());
    ar.getIssues().setIssueList(result.getIssuesAsList());
  }

  /**
   *
   */
  private static void parseAndsetAccessUri(Audubon a, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(a::setAccessUri);
  }

  /**
   *
   */
  private static void parseAndsetFurtherInformationUrl(Audubon a, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(a::setFurtherInformationUrl);
  }

  /**
   *
   */
  private static void parseAndsetAttributionLinkUrl(Audubon a, String v) {
    URI uri = UrlParser.parse(v);
    Optional.ofNullable(uri).map(URI::toString).ifPresent(a::setAttributionLinkUrl);
  }

  /**
   *
   */
  private static void parseAndSetFormat(Audubon a, String v) {
    String mimeType = MEDIA_PARSER.parseMimeType(v);
    if (Strings.isNullOrEmpty(mimeType)) {
      mimeType = MEDIA_PARSER.parseMimeType(a.getIdentifier());
    }
    a.setFormat(mimeType);
  }
}
