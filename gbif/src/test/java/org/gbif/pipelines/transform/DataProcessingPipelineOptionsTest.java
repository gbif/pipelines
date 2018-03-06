package org.gbif.pipelines.transform;

import org.gbif.pipelines.config.DataPipelineOptionsFactory;
import org.gbif.pipelines.config.DataProcessingPipelineOptions;

import org.junit.Assert;
import org.junit.Test;

/**
 * DataprocessingPipelineOptionsTest
 */
public class DataProcessingPipelineOptionsTest {

  private static final String HOST1="c3n1.gbif.org:9200";
  private static final String HOST2="c3n2.gbif.org:9200";

  private static final String DEF_HOST1="http://c4n1.gbif.org:9200";
  private static final String DEF_HOST2="http://c4n2.gbif.org:9200";

  private static final String DEF_INDEX="interpreted-occurrence";
  private static final String DEF_TYPE="es-type";
  private static final Integer DEF_BATCH_SIZE=1000;

  private static final String MYINDEX="myindex";
  private static final String MYTYPE="mytype";
  private static final Integer BATCHSIZE=100001;

  /**
   * Testing Default ESProperties
   */
  @Test
  public void testDefaultESProps(){
    String[] args = new String[]{"--datasetId=xyz"};
    DataProcessingPipelineOptions dataProcessingPipelineOptions = DataPipelineOptionsFactory.create(args);

    Assert.assertEquals(9,dataProcessingPipelineOptions.getESHosts().length);
    Assert.assertEquals(DEF_HOST1,dataProcessingPipelineOptions.getESHosts()[0]);
    Assert.assertEquals(DEF_HOST2,dataProcessingPipelineOptions.getESHosts()[1]);

    Assert.assertEquals(DEF_INDEX,dataProcessingPipelineOptions.getESIndex());
    Assert.assertEquals(DEF_TYPE,dataProcessingPipelineOptions.getESType());
    Assert.assertEquals(DEF_BATCH_SIZE,dataProcessingPipelineOptions.getESMaxBatchSize());
  }

  /**
   * Testing new provided ESProperties
   */
  @Test
  public void testESHostsTypeIndexAndBatchSize(){
    String[] args = new String[]{"--ESHosts=c3n1.gbif.org:9200,c3n2.gbif.org:9200","--datasetId=xyz", "--ESIndex=myindex", "--ESType=mytype","--ESMaxBatchSize=100001"};
    DataProcessingPipelineOptions dataProcessingPipelineOptions = DataPipelineOptionsFactory.create(args);

    Assert.assertEquals(2,dataProcessingPipelineOptions.getESHosts().length);
    Assert.assertEquals(HOST1,dataProcessingPipelineOptions.getESHosts()[0]);
    Assert.assertEquals(HOST2,dataProcessingPipelineOptions.getESHosts()[1]);

    Assert.assertEquals(MYINDEX,dataProcessingPipelineOptions.getESIndex());
    Assert.assertEquals(MYTYPE,dataProcessingPipelineOptions.getESType());
    Assert.assertEquals(BATCHSIZE,dataProcessingPipelineOptions.getESMaxBatchSize());

  }


}
