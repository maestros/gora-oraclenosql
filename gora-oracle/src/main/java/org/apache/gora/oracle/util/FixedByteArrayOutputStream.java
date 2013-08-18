package org.apache.gora.oracle.util;

import java.io.IOException;
import java.io.OutputStream;

public class FixedByteArrayOutputStream extends OutputStream {

  private int i;
  byte out[];

  public FixedByteArrayOutputStream(byte out[]) {
    this.out = out;
  }

  @Override
  public void write(int b) throws IOException {
    out[i++] = (byte) b;
  }

  @Override
  public void write(byte b[], int off, int len) throws IOException {
    System.arraycopy(b, off, out, i, len);
    i += len;
  }

}