package org.gbif.pipelines.core.interpreters.metadata;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.api.model.registry.Endpoint;
import org.gbif.api.model.registry.MachineTag;
import org.gbif.api.util.comparators.EndpointPriorityComparator;
import org.gbif.api.vocabulary.EndpointType;
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
  public static MetadataRecord interpret(
      MetadataServiceClient client, String datasetId, Integer attempt, MetadataRecord mdr) {

    if (client == null) {
      throw new IllegalStateException("MetadataServiceClient is not initialized!");
    }

    // Set required metadata properties
    mdr.setDatasetKey(datasetId);

    Dataset dataset = client.getDataset(datasetId);

    // https://github.com/gbif/pipelines/issues/401
    License license = getLicense(dataset.getLicense());
    if (license == null || license == License.UNSPECIFIED || license == License.UNSUPPORTED) {
      throw new IllegalArgumentException("Dataset licence can't be UNSPECIFIED or UNSUPPORTED!");
    } else {
      mdr.setLicense(license.name());
    }

    mdr.setDatasetTitle(dataset.getTitle());
    mdr.setInstallationKey(dataset.getInstallationKey());
    mdr.setPublishingOrganizationKey(dataset.getPublishingOrganizationKey());

    crawledEndpoint(prioritySortEndpoints(dataset.getEndpoints()))
        .ifPresent(endpoint -> mdr.setProtocol(endpoint.getType().name()));

    List<Network> networkList = client.getNetworkFromDataset(datasetId);
    if (networkList != null && !networkList.isEmpty()) {
      mdr.setNetworkKeys(
          networkList.stream()
              .map(Network::getKey)
              .filter(Objects::nonNull)
              .collect(Collectors.toList()));
    }

    if (dataset.getCategory() != null && !dataset.getCategory().isEmpty()) {
      mdr.setDatasetCategory(new ArrayList<>(dataset.getCategory()));
    }

    Organization organization = client.getOrganization(dataset.getPublishingOrganizationKey());
    mdr.setEndorsingNodeKey(organization.getEndorsingNodeKey());
    mdr.setPublisherTitle(organization.getTitle());
    mdr.setDatasetPublishingCountry(organization.getCountry());

    getLastCrawledDate(dataset.getMachineTags()).ifPresent(d -> mdr.setLastCrawled(d.getTime()));
    if (Objects.nonNull(dataset.getProject())) {
      mdr.setProjectId(dataset.getProject().getIdentifier());
      if (Objects.nonNull(dataset.getProject().getProgramme())) {
        mdr.setProgrammeAcronym(dataset.getProject().getProgramme().getAcronym());
      }
    }

    Installation installation = client.getInstallation(dataset.getInstallationKey());
    mdr.setHostingOrganizationKey(installation.getOrganizationKey());

    mdr.setCrawlId(attempt);

    copyMachineTags(dataset.getMachineTags(), mdr);

    return mdr;
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

  /** Crawlable endpoints, best first. Same order as {@code CrawlerCoordinatorServiceImpl}. */
  public static List<Endpoint> prioritySortEndpoints(List<Endpoint> endpoints) {
    return endpoints.stream()
        .filter(endpoint -> EndpointPriorityComparator.PRIORITIES.contains(endpoint.getType()))
        .sorted(Collections.reverseOrder(new EndpointPriorityComparator()))
        .collect(Collectors.toList());
  }

  /**
   * Endpoint the crawler would use. EML outranks DwC-DP and COLDP but has no records, so a data
   * package is crawled instead and EML stays metadata-only.
   */
  private static Optional<Endpoint> crawledEndpoint(List<Endpoint> bestFirst) {
    if (bestFirst.isEmpty()) {
      return Optional.empty();
    }
    Endpoint best = bestFirst.get(0);
    if (best.getType() == EndpointType.EML) {
      for (Endpoint candidate : bestFirst) {
        EndpointType type = candidate.getType();
        if (type == EndpointType.DWC_DP || type == EndpointType.COLDP) {
          return Optional.of(candidate);
        }
      }
    }
    return Optional.of(best);
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
