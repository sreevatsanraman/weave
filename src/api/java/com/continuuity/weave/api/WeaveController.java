package com.continuuity.weave.api;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.concurrent.Future;

/**
 *
 */
public interface WeaveController {

  RunInfo getRunInfo();

  ListenableFuture<?> stop();

  ListenableFuture<?> sendCommand(Command command);
}
