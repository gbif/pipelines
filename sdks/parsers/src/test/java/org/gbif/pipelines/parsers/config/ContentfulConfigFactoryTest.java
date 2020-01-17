package org.gbif.pipelines.parsers.config;

import java.util.Properties;

import org.gbif.pipelines.parsers.config.factory.ContentfulConfigFactory;
import org.gbif.pipelines.parsers.config.model.ElasticsearchContentConfig;

import org.junit.Assert;
import org.junit.Test;

public class ContentfulConfigFactoryTest {

  @Test(expected = IllegalArgumentException.class)
  public void emptyPropertiesTest() {

    // State
    Properties properties = new Properties();

    // When
    ContentfulConfigFactory.create(properties);

  }

  @Test(expected = IllegalArgumentException.class)
  public void emptyPropertiyValueTest() {

    // State
    Properties properties = new Properties();
    properties.put(ContentfulConfigFactory.CONTENTFUL_ELASTICSEARCH, "");

    // When
    ContentfulConfigFactory.create(properties);
  }

  @Test
  public void arrayPropertiyValueTest() {

    // Expected
    String[] expected = {"1", "2", "3", "4", "5"};

    // State
    Properties properties = new Properties();
    properties.put(ContentfulConfigFactory.CONTENTFUL_ELASTICSEARCH, "1,2,3,4,5");

    // When
    ElasticsearchContentConfig config = ContentfulConfigFactory.create(properties);

    // Should
    Assert.assertNotNull(config);
    Assert.assertNotNull(config.getHosts());
    Assert.assertArrayEquals(expected, config.getHosts());
  }

  @Test
  public void onePropertiyValueTest() {

    // Expected
    String[] expected = {"12345"};

    // State
    Properties properties = new Properties();
    properties.put(ContentfulConfigFactory.CONTENTFUL_ELASTICSEARCH, "12345");

    // When
    ElasticsearchContentConfig config = ContentfulConfigFactory.create(properties);

    // Should
    Assert.assertNotNull(config);
    Assert.assertNotNull(config.getHosts());
    Assert.assertArrayEquals(expected, config.getHosts());

  }

}
