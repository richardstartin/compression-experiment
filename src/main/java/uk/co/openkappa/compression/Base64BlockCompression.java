package uk.co.openkappa.compression;

import java.util.function.Function;

import static java.util.Arrays.asList;
import static uk.co.openkappa.compression.CompressionBenchmark.benchmark;

public class Base64BlockCompression {

  public static void main(String[] args) {
    var encodings = asList(
            Encoding.of("binary", Function.identity()),
            Encoding.of("base64", Util::base64)
    );
    var compressions = asList(
            new Uncompressed<byte[]>(),
            new Gzip<byte[]>(),
            new LZ4<byte[]>( 1536 * 1024 * 1024),
            new Snappy<byte[]>(1536 * 1024 * 1024)
    );
    benchmark(encodings, compressions, Util::randomKiloBytes, 1_000_000);
    benchmark(encodings, compressions, Util::monotonicKiloBytes, 1_000_000);
  }



}
