package org.gbif.pipelines.core.ws.client.internal;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Response structure of the aggregated gbif terms
 */
public final class DatasetMetaInfoResponse implements Serializable {

  private static final long serialVersionUID = -9137655613118727430L;

  private String datasetKey;
  private String publishingCountry;
  private String protocol;
  private String publishingOrgKey;
  private List<String> networkKey;
  private String datasetTitle;

  private DatasetMetaInfoResponse(){}

  public static DatasetMetaInfoResponseBuilder newBuilder(){
    return new DatasetMetaInfoResponseBuilder();
  }

  public String getDatasetKey() {
    return datasetKey;
  }

  private void setDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
  }

  public String getPublishingCountry() {
    return publishingCountry;
  }

  private void setPublishingCountry(String publishingCountry) {
    this.publishingCountry = publishingCountry;
  }

  public String getProtocol() {
    return protocol;
  }

  private void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getPublishingOrgKey() {
    return publishingOrgKey;
  }

  private void setPublishingOrgKey(String publishingOrgKey) {
    this.publishingOrgKey = publishingOrgKey;
  }

  public List<String> getNetworkKey() {
    return networkKey;
  }

  private void setNetworkKey(List<String> networkKey) {
    this.networkKey = networkKey;
  }

  public String getDatasetTitle() {
    return datasetTitle;
  }

  private void setDatasetTitle(String datasetTitle) {
    this.datasetTitle = datasetTitle;
  }



  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatasetMetaInfoResponse that = (DatasetMetaInfoResponse) o;
    return Objects.equal(datasetKey, that.datasetKey)
           && Objects.equal(publishingCountry, that.publishingCountry)
           && Objects.equal(protocol, that.protocol)
           && Objects.equal(publishingOrgKey, that.publishingOrgKey)
           && Objects.equal(networkKey, that.networkKey)
           && Objects.equal(datasetTitle, that.datasetTitle);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(datasetKey, publishingCountry, protocol, publishingOrgKey, networkKey, datasetTitle);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
      .add("datasetKey", datasetKey)
      .add("publishingCountry", publishingCountry)
      .add("protocol", protocol)
      .add("publishingOrgKey", publishingOrgKey)
      .add("networkKey", networkKey)
      .add("datasetTitle", datasetTitle)
      .toString();
  }

  static final class DatasetMetaInfoResponseBuilder {

    private final DatasetMetaInfoResponse datasetMetaInfoResponse;

    private DatasetMetaInfoResponseBuilder() { datasetMetaInfoResponse = new DatasetMetaInfoResponse(); }

    public DatasetMetaInfoResponseBuilder using(String datasetKey) {
      datasetMetaInfoResponse.setDatasetKey(datasetKey);
      return this;
    }

    public DatasetMetaInfoResponseBuilder addPublishingCountry(String publishingCountry) {
      datasetMetaInfoResponse.setPublishingCountry(publishingCountry);
      return this;
    }

    public DatasetMetaInfoResponseBuilder addProtocol(String protocol) {
      datasetMetaInfoResponse.setProtocol(protocol);
      return this;
    }

    public DatasetMetaInfoResponseBuilder addPublishingOrgKey(String publishingOrgKey) {
      datasetMetaInfoResponse.setPublishingOrgKey(publishingOrgKey);
      return this;
    }

    public DatasetMetaInfoResponseBuilder addNetworkKey(List<String> networkKey) {
      datasetMetaInfoResponse.setNetworkKey(networkKey);
      return this;
    }

    public DatasetMetaInfoResponseBuilder addDatasetTitle(String datasetTitle) {
      datasetMetaInfoResponse.setDatasetTitle(datasetTitle);
      return this;
    }

    public DatasetMetaInfoResponse build() { return datasetMetaInfoResponse; }
  }
}
