package com.continuuity.weave.api;

import com.continuuity.weave.api.logging.LogHandler;

/**
 *
 */
public interface WeavePreparer {

  WeavePreparer addLogHandler(LogHandler handler);

  WeavePreparer addErrorHandler();

  WeavePreparer setResourceSpecification(ResourceSpecification resourceSpec);

  WeaveController start();
}
