package org.gbif.pipelines.core.interpreters.model;

import org.jetbrains.annotations.NotNull;

import java.util.List;

public interface MetadataRecord {
    void setDatasetKey(String datasetId);

    void setLicense(String name);

    void setDatasetTitle(String title);

    void setInstallationKey(String installationKey);

    void setPublishingOrganizationKey(String publishingOrganizationKey);

    void setProtocol(String name);

    void setNetworkKeys(List<String> collect);

    void setEndorsingNodeKey(String endorsingNodeKey);

    void setPublisherTitle(String title);

    void setDatasetPublishingCountry(String country);

    void setLastCrawled(long time);

    void setProjectId(String identifier);

    void setProgrammeAcronym(String acronym);

    void setHostingOrganizationKey(String organizationKey);

    void setCrawlId(@NotNull Integer integer);

    void setMachineTags(List<Object> collect);

    String getDatasetPublishingCountry();

    String getDatasetKey();
}
