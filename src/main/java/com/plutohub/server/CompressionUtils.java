package com.plutohub.server;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

public class CompressionUtils {
  public static long count       = 0;
  public static long totalInput  = 0;
  public static long totalOutput = 0;

  final static byte[] buffer = new byte[1024 * 10];

  public static synchronized byte[] compress(final byte[] data) throws IOException {
    final Deflater deflater = new Deflater();
    deflater.setInput(data);

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);

    deflater.finish();
    while (!deflater.finished()) {
      final int count = deflater.deflate(buffer); // returns the generated code... index
      outputStream.write(buffer, 0, count);
    }
    outputStream.close();
    final byte[] output = outputStream.toByteArray();

    ++count;
    totalInput += data.length;
    totalOutput += output.length;

    return output;
  }

  public static synchronized byte[] decompress(final byte[] data) throws IOException, DataFormatException {
    Inflater inflater = new Inflater();
    inflater.setInput(data);

    final ByteArrayOutputStream outputStream = new ByteArrayOutputStream(data.length);
    while (!inflater.finished()) {
      final int count = inflater.inflate(buffer);
      outputStream.write(buffer, 0, count);
    }
    outputStream.close();
    final byte[] output = outputStream.toByteArray();

    return output;
  }
}