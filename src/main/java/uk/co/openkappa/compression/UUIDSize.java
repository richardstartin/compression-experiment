package uk.co.openkappa.compression;

import java.util.UUID;

import static java.nio.ByteBuffer.allocate;
import static java.nio.charset.StandardCharsets.*;

public class UUIDSize {

  public static void main(String[] args) {
    var uuid = UUID.randomUUID();
    var uuidString = uuid.toString();
    var binary = allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array();
    var uuidUTF8 = uuidString.getBytes(UTF_8);
    var uuidUTF16 = uuidString.getBytes(UTF_16);
    var uuidLatin1 = uuidString.getBytes(ISO_8859_1);
    System.out.println("Format,Size (bytes),Ratio ");
    System.out.println("binary" + "," + binary.length + "," + 1);
    System.out.println("ISO-8859-1" + "," + uuidLatin1.length + "," + ((double)uuidLatin1.length / binary.length));
    System.out.println("UTF-8" + "," + uuidUTF8.length + "," + ((double)uuidUTF8.length / binary.length));
    System.out.println("UTF-16" + "," + uuidUTF16.length + "," + ((double)uuidUTF16.length / binary.length));
  }
}
