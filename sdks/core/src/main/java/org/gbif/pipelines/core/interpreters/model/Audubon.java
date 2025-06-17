package org.gbif.pipelines.core.interpreters.model;

public interface Audubon {


    String getCreator();
    void setCreator(String creator);

    String getCreatorUri();
    void setCreatorUri(String creatorUri);

    String getProviderLiteral();
    void setProviderLiteral(String providerLiteral);

    String getProvider();
    void setProvider(String provider);

    String getMetadataCreatorLiteral();
    void setMetadataCreatorLiteral(String metadataCreatorLiteral);

    String getMetadataCreator();
    void setMetadataCreator(String metadataCreator);

    String getMetadataProviderLiteral();
    void setMetadataProviderLiteral(String metadataProviderLiteral);

    String getMetadataProvider();
    void setMetadataProvider(String metadataProvider);

    String getRights();
    void setRights(String rights);

    String getRightsUri();
    void setRightsUri(String rightsUri);

    String getOwner();
    void setOwner(String owner);

    String getUsageTerms();
    void setUsageTerms(String usageTerms);

    String getWebStatement();
    void setWebStatement(String webStatement);

    String getLicenseLogoUrl();
    void setLicenseLogoUrl(String licenseLogoUrl);

    String getCredit();
    void setCredit(String credit);

    String getAttributionLogoUrl();
    void setAttributionLogoUrl(String attributionLogoUrl);

    String getAttributionLinkUrl();
    void setAttributionLinkUrl(String attributionLinkUrl);

    String getFundingAttribution();
    void setFundingAttribution(String fundingAttribution);

    String getSource();
    void setSource(String source);

    String getSourceUri();
    void setSourceUri(String sourceUri);

    String getDescription();
    void setDescription(String description);

    String getCaption();
    void setCaption(String caption);

    String getLanguage();
    void setLanguage(String language);

    String getLanguageUri();
    void setLanguageUri(String languageUri);

    String getPhysicalSetting();
    void setPhysicalSetting(String physicalSetting);

    String getCvTerm();
    void setCvTerm(String cvTerm);

    String getSubjectCategoryVocabulary();
    void setSubjectCategoryVocabulary(String subjectCategoryVocabulary);

    String getTag();
    void setTag(String tag);

    String getLocationShown();
    void setLocationShown(String locationShown);

    String getWorldRegion();
    void setWorldRegion(String worldRegion);

    String getCountryCode();
    void setCountryCode(String countryCode);

    String getCountryName();
    void setCountryName(String countryName);

    String getProvinceState();
    void setProvinceState(String provinceState);

    String getCity();
    void setCity(String city);

    String getSublocation();
    void setSublocation(String sublocation);

    String getIdentifier();
    void setIdentifier(String identifier);

    String getType();
    void setType(String type);

    String getTypeUri();
    void setTypeUri(String typeUri);

    String getSubtypeLiteral();
    void setSubtypeLiteral(String subtypeLiteral);

    String getSubtype();
    void setSubtype(String subtype);

    String getTitle();
    void setTitle(String title);

    String getModified();
    void setModified(String modified);

    String getMetadataDate();
    void setMetadataDate(String metadataDate);

    String getMetadataLanguageLiteral();
    void setMetadataLanguageLiteral(String metadataLanguageLiteral);

    String getMetadataLanguage();
    void setMetadataLanguage(String metadataLanguage);

    String getProviderManagedId();
    void setProviderManagedId(String providerManagedId);

    String getRating();
    void setRating(String rating);

    String getCommenterLiteral();
    void setCommenterLiteral(String commenterLiteral);

    String getCommenter();
    void setCommenter(String commenter);

    String getComments();
    void setComments(String comments);

    String getReviewerLiteral();
    void setReviewerLiteral(String reviewerLiteral);

    String getReviewer();
    void setReviewer(String reviewer);

    String getReviewerComments();
    void setReviewerComments(String reviewerComments);

    String getAvailable();
    void setAvailable(String available);

    String getHasServiceAccessPoint();
    void setHasServiceAccessPoint(String hasServiceAccessPoint);

    String getIdOfContainingCollection();
    void setIdOfContainingCollection(String idOfContainingCollection);

    String getRelatedResourceId();
    void setRelatedResourceId(String relatedResourceId);

    String getProviderId();
    void setProviderId(String providerId);

    String getDerivedFrom();
    void setDerivedFrom(String derivedFrom);

    String getAssociatedSpecimenReference();
    void setAssociatedSpecimenReference(String associatedSpecimenReference);

    String getAssociatedObservationReference();
    void setAssociatedObservationReference(String associatedObservationReference);

    String getLocationCreated();
    void setLocationCreated(String locationCreated);

    String getDigitizationDate();
    void setDigitizationDate(String digitizationDate);

    String getCaptureDevice();
    void setCaptureDevice(String captureDevice);

    String getResourceCreationTechnique();
    void setResourceCreationTechnique(String resourceCreationTechnique);

    String getAccessUri();
    void setAccessUri(String accessUri);

    String getFormat();
    void setFormat(String format);

    String getFormatUri();
    void setFormatUri(String formatUri);

    String getVariantLiteral();
    void setVariantLiteral(String variantLiteral);

    String getVariant();
    void setVariant(String variant);

    String getVariantDescription();
    void setVariantDescription(String variantDescription);

    String getFurtherInformationUrl();
    void setFurtherInformationUrl(String furtherInformationUrl);

    String getLicensingException();
    void setLicensingException(String licensingException);

    String getServiceExpectation();
    void setServiceExpectation(String serviceExpectation);

    String getHashFunction();
    void setHashFunction(String hashFunction);

    String getHashValue();
    void setHashValue(String hashValue);

    String getPixelXDimension();
    void setPixelXDimension(String pixelXDimension);

    String getPixelYDimension();
    void setPixelYDimension(String pixelYDimension);

    String getTaxonCoverage();
    void setTaxonCoverage(String taxonCoverage);

    String getScientificName();
    void setScientificName(String scientificName);

    String getIdentificationQualifier();
    void setIdentificationQualifier(String identificationQualifier);

    String getVernacularName();
    void setVernacularName(String vernacularName);

    String getNameAccordingTo();
    void setNameAccordingTo(String nameAccordingTo);

    String getScientificNameId();
    void setScientificNameId(String scientificNameId);

    String getOtherScientificName();
    void setOtherScientificName(String otherScientificName);

    String getIdentifiedBy();
    void setIdentifiedBy(String identifiedBy);

    String getDateIdentified();
    void setDateIdentified(String dateIdentified);

    String getTaxonCount();
    void setTaxonCount(String taxonCount);

    String getSubjectPart();
    void setSubjectPart(String subjectPart);

    String getSex();
    void setSex(String sex);

    String getLifeStage();
    void setLifeStage(String lifeStage);

    String getSubjectOrientation();
    void setSubjectOrientation(String subjectOrientation);

    String getPreparations();
    void setPreparations(String preparations);

    String getTemporal();
    void setTemporal(String temporal);

    String getCreateDate();
    void setCreateDate(String createDate);

    String getTimeOfDay();
    void setTimeOfDay(String timeOfDay);    
}
