package org.gbif.pipelines.crawler.validator.validate;

import org.gbif.common.messaging.api.messages.PipelineBasedMessage;

public interface ArchiveValidator {

  PipelineBasedMessage createOutgoingMessage();

  void validate();
}
