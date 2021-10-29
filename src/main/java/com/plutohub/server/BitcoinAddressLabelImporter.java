package com.plutohub.server;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;

import java.io.*;
import java.util.concurrent.atomic.*;

public class BitcoinAddressLabelImporter extends DatabaseJob {
  // SETTINGS, THE USER CAN OVERRIDE BY SETTING PROPERTIES AT JVM LEVEL
  // A simple method with everything in it
  public void parseCSV(final String csvFilePath) throws IOException {
    final File csvFile = new File(csvFilePath);
    if (!csvFile.exists())
      throw new IllegalArgumentException("CSV file '" + csvFilePath + "' does not exist");

    try (DatabaseFactory factory = new DatabaseFactory("bitcoin")) {
      database = factory.open();

      try {
        final long beginTime = System.currentTimeMillis();

        prepareAsync();

        final AtomicLong created = new AtomicLong();
        final AtomicLong updated = new AtomicLong();

        System.out.printf("Importing addresses from CSV file '%s'", csvFilePath);

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
          String line;
          while ((line = br.readLine()) != null) {
            String[] values = line.split(",");

            final Vertex address;

            final IndexCursor cursor = database.lookupByKey(BitcoinSchema.VERTEX_ADDRESS, "hash", values[1]);
            if (!cursor.hasNext()) {
              address = database.newVertex(BitcoinSchema.VERTEX_ADDRESS).set("hash", values[1]).set("label", values[2]);
              database.async().createRecord((MutableVertex) address, (rec) -> {
                created.incrementAndGet();
              });
            } else
              address = cursor.next().asVertex();

            if (!values[2].equals(address.get("label"))) {
              // NOT EQUALS
              final MutableVertex modified = address.modify().set("label", values[2]);
              database.async().updateRecord(modified, (rec) -> {
                updated.incrementAndGet();
              });
            }
          }
        }

        database.async().waitCompletion();

        System.out.println(
            "- completed in " + ((System.currentTimeMillis() - beginTime) / 1000) + " secs updated " + updated + " vertices, created " + created + " vertices");

      } finally {
        database.close();
      }
    }

  }

  public static void main(final String[] args) throws IOException {
    if (args.length == 0) {
      printHelp();
      return;
    }

    final BitcoinAddressLabelImporter tb = new BitcoinAddressLabelImporter();

    tb.parseCSV(args[0]);
  }

  private static void printHelp() {
    System.out.println("PLUTOHASH Bitcoin Address Label Importer v21.8.1");
    System.out.println("\nSyntax: address-label-importer.sh <csv-file>");
    System.out.println();
  }
}