package uk.co.openkappa.compression;

import java.util.List;
import java.util.UUID;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.util.Arrays.asList;
import static uk.co.openkappa.compression.CompressionBenchmark.benchmark;

public class UUIDBlockCompression {

  public static void main(String[] args) {
    List<Encoding<UUID>> encodings = asList(
            Encoding.of("binary", UUIDBlockCompression::binary),
            Encoding.of(ISO_8859_1.name(), UUIDBlockCompression::latin1),
            Encoding.of(UTF_16.name(), UUIDBlockCompression::utf16)
    );
    List<Compression<UUID>> compressions = asList(
            new LZ4<>(256 * 1024 * 1024),
            new Uncompressed<>(),
            new Gzip<>(),
            new Snappy<>(256 * 1024 * 1024)
    );
    benchmark(encodings, compressions,  UUIDBlockCompression::uuids, 1_000_000);
  }

  private static Stream<UUID> uuids(int count) {
    return IntStream.range(0, count).mapToObj(i -> UUID.randomUUID());
  }

  private static byte[] binary(UUID uuid) {
    return allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
  }

  private static byte[] latin1(UUID uuid) {
    return uuid.toString().getBytes(ISO_8859_1);
  }

  private static byte[] utf16(UUID uuid) {
    return uuid.toString().getBytes(UTF_16);
  }

}
