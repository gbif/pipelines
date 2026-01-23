package org.gbif.pipelines.core.interpreters.metadata;

import java.net.URI;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.api.vocabulary.License;
import org.gbif.api.vocabulary.TagName;
import org.gbif.common.parsers.LicenseParser;
import org.gbif.pipelines.core.ws.metadata.MetadataServiceClient;
import org.gbif.pipelines.core.ws.metadata.response.Dataset;
import org.gbif.pipelines.core.ws.metadata.response.Installation;
import org.gbif.pipelines.core.ws.metadata.response.Network;
import org.gbif.pipelines.core.ws.metadata.response.Organization;
import org.gbif.pipelines.io.avro.MetadataRecord;

/** Interprets GBIF metadata by datasetId */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class MetadataInterpreter {

  /** Gets information from GBIF API by datasetId */
  public static BiConsumer<String, MetadataRecord> interpret(MetadataServiceClient client) {
    return (datasetId, mdr) -> {

      // Set required metadata properties
      mdr.setDatasetKey(datasetId);

      if (client != null) {

        Dataset dataset = client.getDataset(datasetId);

        // https://github.com/gbif/pipelines/issues/401
        License license = getLicense(dataset.getLicense());
        if (license == null || license == License.UNSPECIFIED || license == License.UNSUPPORTED) {
          throw new IllegalArgumentException(
              "Dataset licence can't be UNSPECIFIED or UNSUPPORTED!");
        } else {
          mdr.setLicense(license.name());
        }

        mdr.setDatasetTitle(dataset.getTitle());
        mdr.setInstallationKey(dataset.getInstallationKey());
        mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());

        List<Endpoint> endpoints = prioritySortEndpoints(dataset.getEndpoints());
        if (!endpoints.isEmpty()) {
          mdr.setProtocol(endpoints.get(0).getType().name());
        }

        List<Network> networkList = client.getNetworkFromDataset(datasetId);
        if (networkList != null && !networkList.isEmpty()) {
          mdr.setNetworkKeys(
              networkList.stream()
                  .map(Network::getKey)
                  .filter(Objects::nonNull)
                  .collect(Collectors.toList()));
        }

        Organization organization = client.getOrganization(dataset.getPublishingOrganizationKey());
        mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
        mdr.setPublisherTitle(organization.getTitle());
        mdr.setDatasetPublishingCountry(organization.getCountry());

        getLastCrawledDate(dataset.getMachineTags())
            .ifPresent(d -> mdr.setLastCrawled(d.getTime()));
        if (Objects.nonNull(dataset.getProject())) {
          mdr.setProjectId(dataset.getProject().getIdentifier());
          if (Objects.nonNull(dataset.getProject().getProgramme())) {
            mdr.setProgrammeAcronym(dataset.getProject().getProgramme().getAcronym());
          }
        }

        Installation installation = client.getInstallation(dataset.getInstallationKey());
        mdr.setHostingOrganizationKey(installation.getOrganizationKey());

        copyMachineTags(dataset.getMachineTags(), mdr);
      }
    };
  }

  /** Returns ENUM instead of url string */
  private static License getLicense(String url) {
    URI uri =
        Optional.ofNullable(url)
            .map(
                x -> {
                  try {
                    return URI.create(x);
                  } catch (IllegalArgumentException ex) {
                    return null;
                  }
                })
            .orElse(null);
    License license = LicenseParser.getInstance().parseUriThenTitle(uri, null);
    // UNSPECIFIED must be mapped to null
    return License.UNSPECIFIED == license ? null : license;
  }

  // Copied from CrawlerCoordinatorServiceImpl
  public static List<Endpoint> prioritySortEndpoints(List<Endpoint> endpoints) {
    // Filter out all Endpoints that we can't crawl
    return endpoints.stream()
        .filter(endpoint -> EndpointPriorityComparator.PRIORITIES.contains(endpoint.getType()))
        .sorted(new EndpointPriorityComparator())
        .collect(Collectors.toList());
  }

  /** Sets attempt number as crawlId */
  public static Consumer<MetadataRecord> interpretCrawlId(Integer attempt) {
    return mdr -> Optional.ofNullable(attempt).ifPresent(mdr::setCrawlId);
  }

  /** Gets the latest crawl attempt time, if exists. */
  public static Optional<Date> getLastCrawledDate(List<MachineTag> machineTags) {
    return Optional.ofNullable(machineTags)
        .flatMap(
            x ->
                x.stream()
                    .filter(
                        tag ->
                            TagName.CRAWL_ATTEMPT.getName().equals(tag.getName())
                                && TagName.CRAWL_ATTEMPT
                                    .getNamespace()
                                    .getNamespace()
                                    .equals(tag.getNamespace()))
                    .sorted(Comparator.comparing(MachineTag::getCreated).reversed())
                    .map(MachineTag::getCreated)
                    .findFirst());
  }

  /** Copy MachineTags into the Avro Metadata record. */
  public static void copyMachineTags(List<MachineTag> machineTags, MetadataRecord mdr) {
    if (Objects.nonNull(machineTags) && !machineTags.isEmpty()) {
      mdr.setMachineTags(
          machineTags.stream()
              .map(
                  machineTag ->
                      org.gbif.pipelines.io.avro.MachineTag.newBuilder()
                          .setNamespace(machineTag.getNamespace())
                          .setName(machineTag.getName())
                          .setValue(machineTag.getValue())
                          .build())
              .collect(Collectors.toList()));
    }
  }
}
