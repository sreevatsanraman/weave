package com.continuuity.weave.api;

import java.net.InetAddress;

/**
 *
 */
public interface WeaveContext {

  String[] getArguments();

  InetAddress getHost();

  WeaveSpecification getSpecification();
}
