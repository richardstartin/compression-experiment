package uk.co.openkappa.compression;

import java.util.List;
import java.util.function.IntFunction;
import java.util.stream.Stream;

public class CompressionBenchmark {

  public static <T> void benchmark(List<Encoding<T>> encodings,
                                   List<Compression<T>> compressions,
                                   IntFunction<Stream<T>> generator,
                                   int count) {
    System.out.println("Compression,Encoding,Count,Compressed Size (MB)");
    for (Compression<T> compression : compressions) {
      for (Encoding<T> encoding : encodings) {
        System.out.println(compression.name() + ","
                + encoding.name() + ","
                + count + ","
                + String.format("%.2f", (compression.bytes(count, generator, encoding) / 1024f / 1024f)));
      }
    }
  }
}
