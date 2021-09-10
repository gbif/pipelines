package org.gbif.validator.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import javax.validation.constraints.Min;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonDeserialize(builder = ValidationSearchRequest.ValidationSearchRequestBuilder.class)
@AllArgsConstructor
public class ValidationSearchRequest {

  public enum SortOrder {
    ASC,
    DESC
  }

  @Data
  @Builder
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonDeserialize(builder = SortBy.SortByBuilder.class)
  public static class SortBy {

    /** Field to order by. */
    private String field;

    /** Sort order of this field. */
    private SortOrder order;

    @Override
    public String toString() {
      return field + ":" + order.name();
    }
  }

  /** Identifier at the source. */
  private String sourceId;

  /** GBIF Installation from where the validation started. */
  private UUID installationKey;

  /** Validation statuses. */
  private Set<Validation.Status> status;

  private List<SortBy> sortBy;

  @Builder.Default
  @Min(0)
  private Long offset = 20L;

  @Builder.Default
  @Min(0)
  private Integer limit = 0;

  public static class ValidationSearchRequestBuilder {

    /** Adds a sort to the list of sortBy elements. */
    private ValidationSearchRequestBuilder addSort(String fieldName, SortOrder order) {
      if (sortBy == null) {
        sortBy = new ArrayList<>();
      }
      sortBy.add(SortBy.builder().field(fieldName).order(order).build());
      return this;
    }

    /** Sort by created date. */
    public ValidationSearchRequestBuilder sortByCreated(SortOrder order) {
      return addSort("created", order);
    }

    /** Sort by key. */
    public ValidationSearchRequestBuilder sortByKey(SortOrder order) {
      return addSort("key", order);
    }
  }
}
