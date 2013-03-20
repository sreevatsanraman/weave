package com.continuuity.weave.api;

/**
 *
 */
public interface WeaveRunnable extends Runnable {

  WeaveSpecification configure();

  void initialize(WeaveContext context);

  void handleCommand(Command command) throws Exception;

  void stop();
}
