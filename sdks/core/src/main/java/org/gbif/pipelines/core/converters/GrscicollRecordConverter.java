package org.gbif.pipelines.core.converters;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.gbif.pipelines.io.avro.grscicoll.Match;
import org.gbif.rest.client.grscicoll.GrscicollLookupResponse;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class GrscicollRecordConverter {

  public static Match convertMatch(GrscicollLookupResponse.Match matchResponse) {
    Match.Builder builder = Match.newBuilder();

    builder.setMatchType(matchResponse.getMatchType().name());
    builder.setStatus(convertStatus(matchResponse.getStatus()));
    builder.setReasons(convertReasons(matchResponse.getReasons()));
    builder.setKey(matchResponse.getEntityMatched().getKey().toString());

    return builder.build();
  }

  private static List<String> convertReasons(Set<GrscicollLookupResponse.Reason> reasons) {
    if (reasons == null || reasons.isEmpty()) {
      return Collections.emptyList();
    }
    return reasons.stream().map(Enum::name).collect(Collectors.toList());
  }

  private static String convertStatus(GrscicollLookupResponse.Status status) {
    return status == null ? null : status.name();
  }
}
