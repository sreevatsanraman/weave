package com.continuuity.weave.api;

import java.util.concurrent.Future;

/**
 *
 */
public interface WeaveController {

  RunInfo getRunInfo();

  Future<?> stop();

  void stopAndWait();

  Future<?> sendCommand(Command command);
}
