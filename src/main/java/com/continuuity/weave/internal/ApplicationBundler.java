/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.continuuity.weave.internal;

import com.continuuity.weave.internal.utils.Dependencies;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import org.objectweb.asm.Type;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

/**
 *
 */
public final class ApplicationBundler {

  private final List<String> excludePackages;
  private final List<String> includePackages;

  public ApplicationBundler(Iterable<String> excludePackages, Iterable<String> includePackages) {
    this.excludePackages = Lists.newArrayList(excludePackages);
    this.includePackages = Lists.newArrayList(includePackages);
  }

  public void createBundle(File targetFile, Iterable<Class<?>> classes) throws IOException {
    final Set<String> entries = Sets.newHashSet();
    final JarOutputStream jarOut = new JarOutputStream(new FileOutputStream(targetFile));
    try {
      findDependencies(classes, entries, jarOut);

      // Add all classes under the packages as listed in includePackages
      for (String pkg : includePackages) {
        findPackageClasses(pkg, entries, jarOut);
      }

    } finally {
      jarOut.close();
    }

  }

  public void createBundle(File targetFile, Class<?> clz, Class<?>...classes) throws IOException {
    createBundle(targetFile, ImmutableSet.<Class<?>>builder().add(clz).add(classes).build());
  }

  private void findDependencies(Iterable<Class<?>> classes,
                                final Set<String> entries, final JarOutputStream jarOut) throws IOException {
    Dependencies.findClassDependencies(ImmutableSet.copyOf(classes), new Predicate<Class<?>>() {
      @Override
      public boolean apply(Class<?> input) {
        ClassLoader classLoader = input.getClassLoader();
        if (classLoader == null) {
          return false;
        }
        String className = input.getName();
        for (String exclude : excludePackages) {
          if (className.startsWith(exclude)) {
            return false;
          }
        }

        String internalName = Type.getInternalName(input);
        putDirEntry(internalName.substring(0, internalName.lastIndexOf('/')), entries, jarOut);
        putClassEntry(input, entries, jarOut);

        return true;
      }
    });
  }

  private void putDirEntry(String path, Set<String> entries, JarOutputStream jarOut) {
    try {
      String entry = "";
      for (String dir : Splitter.on('/').split(path)) {
        entry += dir + '/';
        if (entries.add(entry)) {
          jarOut.putNextEntry(new JarEntry(entry));
          jarOut.closeEntry();
        }
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void putClassEntry(Class<?> clz, Set<String> entries, JarOutputStream jarOut) {
    try {
      String entry = Type.getInternalName(clz) + ".class";
      if (entries.add(entry)) {
        jarOut.putNextEntry(new JarEntry(entry));
        InputStream is = clz.getClassLoader().getResourceAsStream(entry);
        ByteStreams.copy(is, jarOut);
        jarOut.closeEntry();
      }
    } catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  private void findPackageClasses(String pkg, Set<String> entries, JarOutputStream jarOut) throws IOException {
    String prefix = pkg.replace('.', '/');

    Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(prefix);
    while (resources.hasMoreElements()) {
      URL url = resources.nextElement();
      if ("jar".equals(url.getProtocol())) {
        copyJarEntries(url, entries, jarOut);
      } else if ("file".equals(url.getProtocol())) {
        String path = url.getFile();
        File baseDir = new File(path.substring(0, path.indexOf(prefix)));
        copyFileEntries(baseDir, prefix, entries, jarOut);
      }
      // Otherwise, ignore the package
    }
  }

  /**
   * Copies all entries under the path specified by the jarPath to the JarOutputStream.
   * @param jarPath
   * @param entries
   * @param jarOut
   * @throws IOException
   */
  private void copyJarEntries(URL jarPath, Set<String> entries, JarOutputStream jarOut) throws IOException {
    String spec = new URL(jarPath.getFile()).getFile();
    String entryRoot = spec.substring(spec.lastIndexOf('!') + 1);
    if (entryRoot.charAt(0) == '/') {
      entryRoot = entryRoot.substring(1);
    }
    JarFile jarFile = new JarFile(spec.substring(0, spec.lastIndexOf('!')));

    try {
      Enumeration<JarEntry> jarEntries = jarFile.entries();
      while (jarEntries.hasMoreElements()) {
        JarEntry jarEntry = jarEntries.nextElement();
        String name = jarEntry.getName();
        if (name == null || !name.startsWith(entryRoot)) {
          continue;
        }
        if (entries.add(name)) {
          jarOut.putNextEntry(new JarEntry(jarEntry));
          ByteStreams.copy(jarFile.getInputStream(jarEntry), jarOut);
          jarOut.closeEntry();
        }
      }
    } finally {
      jarFile.close();
    }
  }

  private void copyFileEntries(File baseDir, String entryRoot,
                               Set<String> entries, JarOutputStream jarOut) throws IOException {
    URI baseUri = baseDir.toURI();
    File packageFile = new File(baseDir, entryRoot);
    Queue<File> queue = Lists.newLinkedList();
    queue.add(packageFile);
    while (!queue.isEmpty()) {
      File file = queue.remove();

      String entry = baseUri.relativize(file.toURI()).getPath();
      if (entries.add(entry)) {
        jarOut.putNextEntry(new JarEntry(entry));
        if (file.isFile()) {
          Files.copy(file, jarOut);
        }
        jarOut.closeEntry();
      }

      if (file.isDirectory()) {
        File[] files = file.listFiles();
        if (files != null) {
          queue.addAll(Arrays.asList(files));
        }
      }
    }
  }
}
