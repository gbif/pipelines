package org.gbif.pipelines.validator.checklists.ws;

import com.fasterxml.jackson.annotation.JsonIgnore;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import jakarta.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.gbif.api.model.checklistbank.ParsedName;
import org.gbif.api.vocabulary.Kingdom;
import org.gbif.api.vocabulary.Rank;
import org.gbif.api.vocabulary.TaxonomicStatus;
import org.gbif.checklistbank.authorship.AuthorComparator;
import org.gbif.nub.lookup.straight.IdLookup;
import org.gbif.nub.lookup.straight.LookupUsage;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.client.ClientContract;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.springframework.cloud.openfeign.annotation.PathVariableParameterProcessor;
import org.springframework.cloud.openfeign.annotation.QueryMapParameterProcessor;
import org.springframework.cloud.openfeign.annotation.RequestHeaderParameterProcessor;
import org.springframework.cloud.openfeign.annotation.RequestParamParameterProcessor;

/** IdLookup service that wraps a remove client that uses a lookup match service. */
public class IdLookupClient implements IdLookup {

  private final AuthorComparator authorComparator = AuthorComparator.createWithAuthormap();

  private final IdLookupWsClient wsClient;

  /** Mixin to ignore fields annotated in Checklistbank classes using an older Jackson. */
  public abstract class LookupUsageMixin {
    @JsonIgnore private Int2IntMap proParteKeys;
  }

  public IdLookupClient(String apiUrl) {
    // A custom client is used to avoid API and Jackson clashes
    wsClient =
        new ClientBuilder()
            .withUrl(apiUrl)
            .withClientContract(
                ClientContract.withProcessors(
                    new PathVariableParameterProcessor(),
                    new RequestParamParameterProcessor(),
                    new RequestHeaderParameterProcessor(),
                    new QueryMapParameterProcessor()))
            .withObjectMapper(
                JacksonJsonObjectMapperProvider.getDefaultObjectMapper()
                    .addMixIn(LookupUsage.class, LookupUsageMixin.class))
            .withExponentialBackoffRetry(Duration.ofSeconds(3L), 2d, 10)
            .build(IdLookupWsClient.class);
  }

  /** Proxies the call to the method that handles all parameters. */
  @Override
  public LookupUsage match(String canonicalName, Rank rank, Kingdom kingdom) {
    return match(canonicalName, null, null, rank, null, kingdom);
  }

  /** Performs the actual call through the WS client. */
  @Override
  public LookupUsage match(
      String canonicalName,
      @Nullable String authorship,
      @Nullable String year,
      Rank rank,
      TaxonomicStatus status,
      Kingdom kingdom,
      IntSet... ignoreIDs) {
    return wsClient.match(canonicalName, authorship, year, rank, status, kingdom);
  }

  @Override
  public LookupUsage exactCurrentMatch(ParsedName parsedName, Kingdom kingdom, IntSet... intSets) {
    return null;
  }

  /** Proxies the call to the method that handles all parameters. */
  @Override
  public List<LookupUsage> match(String canonicalName) {
    return Collections.singletonList(match(canonicalName, null, null, null, null, null));
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int deletedIds() {
    return 0;
  }

  @Override
  public Iterator<LookupUsage> iterator() {
    return null;
  }

  @Nullable
  @Override
  public AuthorComparator getAuthorComparator() {
    return authorComparator;
  }

  @Override
  public void close() throws Exception {}
}
