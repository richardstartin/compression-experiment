package uk.co.openkappa.uuid;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.ISO_8859_1;
import static java.nio.charset.StandardCharsets.UTF_16;
import static java.util.Arrays.asList;

public class UUIDBlockCompression {

  public static void main(String[] args) {
    List<Encoding> encodings = asList(
            Encoding.of("binary", UUIDBlockCompression::binary),
            Encoding.of(ISO_8859_1.name(), UUIDBlockCompression::latin1),
            Encoding.of(UTF_16.name(), UUIDBlockCompression::utf16)
    );
    List<Compression> compressions = asList(
            new Uncompressed(),
            new Gzip(),
            new LZ4(),
            new Snappy()
    );
    System.out.println("Compression,Encoding,Count,Compressed Size (MB)");
    for (Compression compression : compressions) {
      for (Encoding encoding : encodings) {
        int count = 1_000_000;
        System.out.println(compression.name() + ","
                + encoding.name() + ","
                + count + ","
                + String.format("%.2f", (compression.bytes(count, encoding) / 1024f / 1024f)));
      }
    }
  }


  private interface Compression {
    String name();
    int bytes(int count, Function<UUID, byte[]> serialiser);
  }

  private interface Encoding extends Function<UUID, byte[]> {
    static Encoding of(String name, Function<UUID, byte[]> serialiser) {
      return new Encoding() {
        @Override
        public String name() {
          return name;
        }

        @Override
        public byte[] apply(UUID uuid) {
          return serialiser.apply(uuid);
        }
      };
    }

    String name();
  }

  private static class LZ4 implements Compression {

    @Override
    public String name() {
      return "LZ4";
    }

    @Override
    public int bytes(int count, Function<UUID, byte[]> serialiser) {
      LZ4Factory factory = LZ4Factory.fastestInstance();
      LZ4Compressor compressor = factory.fastCompressor();
      ByteBuffer buffer = ByteBuffer.allocate(256 * 1024 * 1024);
      uuids(count).map(serialiser).forEach(buffer::put);
      ByteBuffer compressed = ByteBuffer.allocate(256 * 1024 * 1024);
      compressor.compress(buffer.flip(), compressed);
      return compressed.position();
    }
  }

  private static class Snappy implements Compression {

    @Override
    public String name() {
      return "Snappy";
    }

    @Override
    public int bytes(int count, Function<UUID, byte[]> serialiser) {
      ByteBuffer buffer = ByteBuffer.allocateDirect(256 * 1024 * 1024);
      ByteBuffer compressed = ByteBuffer.allocateDirect(256 * 1024 * 1024);
      uuids(count).map(serialiser).forEach(buffer::put);
      try {
        org.xerial.snappy.Snappy.compress(buffer.flip(), compressed);
        return compressed.limit();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  private static class Uncompressed implements Compression {

    @Override
    public String name() {
      return "Uncompressed";
    }

    @Override
    public int bytes(int count, Function<UUID, byte[]> serialiser) {
      return uuids(count).map(serialiser).mapToInt(b -> b.length).sum();
    }
  }

  private static class Gzip implements Compression {

    @Override
    public String name() {
      return "GZIP";
    }

    @Override
    public int bytes(int count, Function<UUID, byte[]> serialiser) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream(256 * 1024 * 1024);
      try (GZIPOutputStream gzos = new GZIPOutputStream(bos)) {
        Iterable<byte[]> data = () -> uuids(count).map(serialiser).iterator();
        for (byte[] d : data) {
          gzos.write(d);
        }
        gzos.finish();
        return bos.size();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
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
