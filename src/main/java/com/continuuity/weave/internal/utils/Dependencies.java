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
package com.continuuity.weave.internal.utils;

import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import org.objectweb.asm.AnnotationVisitor;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.signature.SignatureReader;
import org.objectweb.asm.signature.SignatureVisitor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Queue;
import java.util.Set;

/**
 * Utility class to help find out class dependencies.
 */
public final class Dependencies {

  /**
   * Finds the class dependencies of the given class.
   * @param classesToResolve Classes for looking for dependencies.
   * @param acceptor Predicate to accept a found class.
   * @throws IOException Thrown where there is error when loading in class bytecode.
   */
  public static void findClassDependencies(Iterable<Class<?>> classesToResolve,
                                           final Predicate<Class<?>> acceptor) throws IOException {

    final Set<String> dependencies = Sets.newHashSet();
    final Queue<Class<?>> classes = Lists.newLinkedList(classesToResolve);

    while (!classes.isEmpty()) {
      Class<?> clz = classes.remove();
      final ClassLoader classLoader = clz.getClassLoader();
      InputStream is = classLoader.getResourceAsStream(Type.getInternalName(clz) + ".class");
      ClassReader classReader = new ClassReader(ByteStreams.toByteArray(is));

      classReader.accept(new DependencyClassVisitor(new DependencyAcceptor() {
        @Override
        public void accept(String className) {
          try {
            Class<?> clz = classLoader.loadClass(className);
            // See if the class is accepted
            if (acceptor.apply(clz)) {
              // If haven't seen this class before, add it to the queue for dependency discovery.
              if (dependencies.add(className)) {
                classes.add(clz);
              }
            }
          } catch (Throwable t) {
            // Ignore classes that cannot be loaded.
          }
        }
      }), ClassReader.SKIP_DEBUG);
    }
  }

  private interface DependencyAcceptor {
    void accept(String className);
  }

  private static final class DependencyClassVisitor extends ClassVisitor {

    private final SignatureVisitor signatureVisitor;
    private final DependencyAcceptor acceptor;

    public DependencyClassVisitor(DependencyAcceptor acceptor) {
      super(Opcodes.ASM4);
      this.acceptor = acceptor;
      this.signatureVisitor = new SignatureVisitor(Opcodes.ASM4) {
        private String currentClass;

        @Override
        public void visitClassType(String name) {
          currentClass = name;
          addClass(name);
        }

        @Override
        public void visitInnerClassType(String name) {
          addClass(currentClass + "$" + name);
        }
      };
    }

    @Override
    public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
      addClass(name);

      if (signature != null) {
        new SignatureReader(signature).accept(signatureVisitor);
      } else {
        addClass(superName);
        addClasses(interfaces);
      }
    }

    @Override
    public void visitOuterClass(String owner, String name, String desc) {
      addClass(owner);
    }

    @Override
    public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
      addType(Type.getType(desc));
      return null;
    }

    @Override
    public void visitInnerClass(String name, String outerName, String innerName, int access) {
      addClass(name);
    }

    @Override
    public FieldVisitor visitField(int access, String name, String desc, String signature, Object value) {
      if (signature != null) {
        new SignatureReader(signature).acceptType(signatureVisitor);
      } else {
        addType(Type.getType(desc));
      }

      return new FieldVisitor(Opcodes.ASM4) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          addType(Type.getType(desc));
          return null;
        }
      };
    }

    @Override
    public MethodVisitor visitMethod(int access, String name, String desc, String signature, String[] exceptions) {
      if (signature != null) {
        new SignatureReader(signature).accept(signatureVisitor);
      } else {
        addMethod(desc);
      }
      addClasses(exceptions);

      return new MethodVisitor(Opcodes.ASM4) {
        @Override
        public AnnotationVisitor visitAnnotation(String desc, boolean visible) {
          addType(Type.getType(desc));
          return null;
        }

        @Override
        public AnnotationVisitor visitParameterAnnotation(int parameter, String desc, boolean visible) {
          addType(Type.getType(desc));
          return null;
        }

        @Override
        public void visitTypeInsn(int opcode, String type) {
          addType(Type.getObjectType(type));
        }

        @Override
        public void visitFieldInsn(int opcode, String owner, String name, String desc) {
          addType(Type.getObjectType(owner));
          addType(Type.getType(desc));
        }

        @Override
        public void visitMethodInsn(int opcode, String owner, String name, String desc) {
          addType(Type.getObjectType(owner));
          addMethod(desc);
        }

        @Override
        public void visitLdcInsn(Object cst) {
          if (cst instanceof Type) {
            addType((Type)cst);
          }
        }

        @Override
        public void visitMultiANewArrayInsn(String desc, int dims) {
          addType(Type.getType(desc));
        }

        @Override
        public void visitLocalVariable(String name, String desc, String signature, Label start, Label end, int index) {
          if (signature != null) {
            new SignatureReader(signature).acceptType(signatureVisitor);
          } else {
            addType(Type.getType(desc));
          }
        }
      };
    }

    private void addClass(String internalName) {
      if (internalName != null) {
        acceptor.accept(Type.getObjectType(internalName).getClassName());
      }
    }

    private void addClasses(String[] classes) {
      if (classes != null) {
        for (String clz : classes) {
          addClass(clz);
        }
      }
    }

    private void addType(Type type) {
      if (type.getSort() == Type.ARRAY) {
        type = type.getElementType();
      }
      if (type.getSort() == Type.OBJECT) {
        addClass(type.getInternalName());
      }
    }

    private void addMethod(String desc) {
      addType(Type.getReturnType(desc));
      for (Type type : Type.getArgumentTypes(desc)) {
        addType(type);
      }
    }
  }

  private Dependencies() {
  }
}
