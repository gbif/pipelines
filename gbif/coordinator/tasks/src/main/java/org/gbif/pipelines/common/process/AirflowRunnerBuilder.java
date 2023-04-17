package org.gbif.pipelines.common.process;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.StringJoiner;
import java.util.function.Consumer;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicNameValuePair;
import org.gbif.pipelines.common.airflow.AirflowBody;
import org.gbif.pipelines.common.airflow.AirflowRunner;
import org.gbif.pipelines.common.configs.AirflowConfiguration;

@Slf4j
@Builder
public class AirflowRunnerBuilder {
  private static final String DELIMITER = " ";
  private String dagId;
  private String dagRunId;
  @NonNull private AirflowConfiguration airflowConfiguration;

  @Builder.Default private Consumer<StringJoiner> beamConfigFn = j -> {};

  public String[] buildOptions() {
    StringJoiner joiner = new StringJoiner(DELIMITER);
    beamConfigFn.accept(joiner);
    return joiner.toString().split(DELIMITER);
  }

  public AirflowRunner buildAirflowRunner() {
    AirflowRunner runner = new AirflowRunner();
    runner.setDagId(dagId);
    runner.setBody(buildBody());

    ArrayList<NameValuePair> headers = new ArrayList<>();

    headers.add(new BasicNameValuePair("Basic", airflowConfiguration.getBasicAuthString()));
    headers.add(
        new BasicNameValuePair(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString()));

    runner.setHeaders(headers);

    return runner;
  }

  private AirflowBody buildBody() {
    AirflowBody body = new AirflowBody(airflowConfiguration);
    body.setArgs(Arrays.asList(buildOptions()));
    return body;
  }
}
