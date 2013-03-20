package com.continuuity.weave.api;

import java.util.Map;

/**
 *
 */
public interface Command {

  String getCommand();

  Map<String, String> getOptions();
}
