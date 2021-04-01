/**
 * Copyright (C) 2014-2016 Open Whisper Systems
 *
 * Licensed according to the LICENSE file in this repository.
 */

package org.whispersystems.signalservice.internal.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.SecureRandom;

public class Util {

  public static byte[][] split(byte[] input, int firstLength, int secondLength) {
    byte[][] parts = new byte[2][];

    parts[0] = new byte[firstLength];
    System.arraycopy(input, 0, parts[0], 0, firstLength);

    parts[1] = new byte[secondLength];
    System.arraycopy(input, firstLength, parts[1], 0, secondLength);

    return parts;
  }

  public static byte[] getSecretBytes(int size) {
    byte[] secret = new byte[size];
    new SecureRandom().nextBytes(secret);
    return secret;
  }

  public static void readFully(InputStream in, byte[] buffer) throws IOException {
    int offset = 0;

    for (;;) {
      int read = in.read(buffer, offset, buffer.length - offset);

      if (read + offset < buffer.length) offset += read;
      else                		           return;
    }
  }

  public static long copy(InputStream in, OutputStream out) throws IOException {
    long copied = 0;
    byte[] buffer = new byte[4096];
    int read;

    while ((read = in.read(buffer)) != -1) {
      copied += read;
      out.write(buffer, 0, read);
    }

    in.close();
    out.close();
    return copied;
  }

  public static boolean streamsEqual(InputStream a, InputStream b) throws IOException {
    byte[] bufferA = new byte[4096];
    byte[] bufferB = new byte[4096];
    int readA;
    int readB;

    try {
      while (true) {
        readA = a.read(bufferA);
        readB = b.read(bufferB);
        if (readA != readB) {
          return false;
        }
        if (readA == -1) {
          return true;
        }
        for (int i=0; i<readA; i++) {
          if (bufferA[i] != bufferB[i]) {
            return false;
          }
        }
      }
    } finally {
      a.close();
      b.close();
    }

  }

  public static int toIntExact(long value) {
    if ((int)value != value) {
      throw new ArithmeticException("integer overflow");
    }
    return (int)value;
  }

}
