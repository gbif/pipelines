package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.grscicoll.Address;
import org.gbif.pipelines.io.avro.grscicoll.Collection;
import org.gbif.pipelines.io.avro.grscicoll.CollectionMatch;
import org.gbif.pipelines.io.avro.grscicoll.Identifier;
import org.gbif.pipelines.io.avro.grscicoll.Institution;
import org.gbif.pipelines.io.avro.grscicoll.InstitutionMatch;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.CollectionResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.InstitutionResponse;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse.Match;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollRecordConverter {

  public static InstitutionMatch convertInstitutionMatch(Match<InstitutionResponse> matchResponse) {
    InstitutionMatch.Builder builder = InstitutionMatch.newBuilder();

    builder.setMatchType(matchResponse.getMatchType().name());
    builder.setStatus(convertStatus(matchResponse.getStatus()));
    builder.setReasons(convertReasons(matchResponse.getReasons()));

    builder.setInstitution(convertInstitution(matchResponse.getEntityMatched()));

    return builder.build();
  }

  public static CollectionMatch convertCollectionMatch(Match<CollectionResponse> matchResponse) {
    CollectionMatch.Builder builder = CollectionMatch.newBuilder();

    builder.setMatchType(matchResponse.getMatchType().name());
    builder.setStatus(convertStatus(matchResponse.getStatus()));
    builder.setReasons(convertReasons(matchResponse.getReasons()));

    builder.setCollection(convertCollection(matchResponse.getEntityMatched()));

    return builder.build();
  }

  private static Institution convertInstitution(InstitutionResponse institutionResponse) {
    Institution.Builder builder = Institution.newBuilder();

    builder.setKey(institutionResponse.getKey().toString());
    builder.setCode(institutionResponse.getCode());
    builder.setName(institutionResponse.getName());
    builder.setAlternativeCodes(institutionResponse.getAlternativeCodes());
    builder.setAddress(convertAddress(institutionResponse.getAddress()));
    builder.setMailingAddress(convertAddress(institutionResponse.getMailingAddress()));

    if (institutionResponse.getIdentifiers() != null
        && !institutionResponse.getIdentifiers().isEmpty()) {
      List<Identifier> identifiers =
          institutionResponse.getIdentifiers().stream()
              .map(GrscicollRecordConverter::convertIdentifier)
              .collect(Collectors.toList());
      builder.setIdentifiers(identifiers);
    }

    return builder.build();
  }

  private static Collection convertCollection(CollectionResponse collectionResponse) {
    Collection.Builder builder = Collection.newBuilder();

    builder.setKey(collectionResponse.getKey().toString());
    builder.setCode(collectionResponse.getCode());
    builder.setName(collectionResponse.getName());
    builder.setInstitutionKey(collectionResponse.getInstitutionKey().toString());
    builder.setAlternativeCodes(collectionResponse.getAlternativeCodes());
    builder.setAddress(convertAddress(collectionResponse.getAddress()));
    builder.setMailingAddress(convertAddress(collectionResponse.getMailingAddress()));

    if (collectionResponse.getIdentifiers() != null
        && !collectionResponse.getIdentifiers().isEmpty()) {
      List<Identifier> identifiers =
          collectionResponse.getIdentifiers().stream()
              .map(GrscicollRecordConverter::convertIdentifier)
              .collect(Collectors.toList());
      builder.setIdentifiers(identifiers);
    }

    return builder.build();
  }

  private static List<String> convertReasons(
      Set<org.gbif.api.model.collections.lookup.Match.Reason> reasons) {
    if (reasons == null || reasons.isEmpty()) {
      return Collections.emptyList();
    }
    return reasons.stream().map(Enum::name).collect(Collectors.toList());
  }

  private static String convertStatus(org.gbif.api.model.collections.lookup.Match.Status status) {
    return status == null ? null : status.name();
  }

  private static Address convertAddress(org.gbif.api.model.collections.Address address) {
    if (address == null) {
      return null;
    }

    Address.Builder builder = Address.newBuilder();

    builder.setAddress(address.getAddress());
    builder.setCity(address.getCity());
    builder.setPostalCode(address.getPostalCode());
    builder.setProvince(address.getProvince());

    if (address.getCountry() != null) {
      builder.setCountry(address.getCountry().getIso2LetterCode());
    }

    return builder.build();
  }

  private static Identifier convertIdentifier(org.gbif.api.model.registry.Identifier identifier) {
    if (identifier == null) {
      return null;
    }

    Identifier.Builder builder = Identifier.newBuilder();

    builder.setIdentifier(identifier.getIdentifier());
    builder.setType(identifier.getType().name());

    return builder.build();
  }
}
