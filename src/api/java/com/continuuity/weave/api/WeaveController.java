package com.continuuity.weave.api;

import java.util.concurrent.Future;

/**
 *
 */
public interface WeaveController {

  Future<?> stop();

  void stopAndWait();

  Future<?> sendCommand(Command command);
}
