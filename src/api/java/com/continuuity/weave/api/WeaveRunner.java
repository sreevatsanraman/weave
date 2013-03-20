package com.continuuity.weave.api;

/**
 *
 */
public interface WeaveRunner {

  WeavePreparer prepare(WeaveRunnable runnable);

  WeaveController lookup(RunId runId);
}
