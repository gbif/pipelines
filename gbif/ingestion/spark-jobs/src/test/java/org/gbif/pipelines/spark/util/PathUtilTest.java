package org.gbif.pipelines.spark.util;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class PathUtilTest {

  @Test
  void crawlAttemptPath() {
    assertEquals("/data/abc123/abc123.1", PathUtil.crawlAttemptPath("/data", "abc123", 1));
  }

  @Test
  void crawlAttemptPathWithTrailingSlash() {
    assertEquals("/data/abc123/abc123.2", PathUtil.crawlAttemptPath("/data/", "abc123", 2));
  }

  @Test
  void interpretedAttemptPath() {
    assertEquals("/data/abc123/1", PathUtil.interpretedAttemptPath("/data", "abc123", 1));
  }

  @Test
  void interpretedAttemptPathWithTrailingSlash() {
    assertEquals("/data/abc123/2", PathUtil.interpretedAttemptPath("/data/", "abc123", 2));
  }
}
