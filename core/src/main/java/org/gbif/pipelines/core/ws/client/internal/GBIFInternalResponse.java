package org.gbif.pipelines.core.ws.client.internal;

import java.io.Serializable;
import java.util.List;

import com.google.common.base.MoreObjects;

public class GBIFInternalResponse implements Serializable {

  private static final long serialVersionUID = -9137655613118727430L;

  private String datasetKey;
  private String publishingCountry;
  private String protocol;
  private String publishingOrgKey;
  private List<String> networkKey;
  private String datasetTitle;

  public static long getSerialVersionUID() {
    return serialVersionUID;
  }

  public String getDatasetKey() {
    return datasetKey;
  }

  public void setDatasetKey(String datasetKey) {
    this.datasetKey = datasetKey;
  }

  public String getPublishingCountry() {
    return publishingCountry;
  }

  public void setPublishingCountry(String publishingCountry) {
    this.publishingCountry = publishingCountry;
  }

  public String getProtocol() {
    return protocol;
  }

  public void setProtocol(String protocol) {
    this.protocol = protocol;
  }

  public String getPublishingOrgKey() {
    return publishingOrgKey;
  }

  public void setPublishingOrgKey(String publishingOrgKey) {
    this.publishingOrgKey = publishingOrgKey;
  }

  public List<String> getNetworkKey() {
    return networkKey;
  }

  public void setNetworkKey(List<String> networkKey) {
    this.networkKey = networkKey;
  }

  public String getDatasetTitle() {
    return datasetTitle;
  }

  public void setDatasetTitle(String datasetTitle) {
    this.datasetTitle = datasetTitle;
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
}
