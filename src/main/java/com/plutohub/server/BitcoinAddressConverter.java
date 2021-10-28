package com.plutohub.server;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.query.sql.executor.ResultSet;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

import static com.plutohub.server.BitcoinSchema.VERTEX_INPUT_TX;
import static com.plutohub.server.BitcoinSchema.VERTEX_OUTPUT_TX;

public class BitcoinAddressConverter extends BitcoinJob {
  private static final int DUMP_PROGRESS_EVERY = 5_000;

  // SETTINGS, THE USER CAN OVERRIDE BY SETTING PROPERTIES AT JVM LEVEL
  private final AtomicLong convertedAddresses = new AtomicLong();

  public void convert() {
    loadSettings();

    try (DatabaseFactory factory = new DatabaseFactory("bitcoin")) {
      database = factory.open();

      try {
        final long beginTime = System.currentTimeMillis();

        commitAndStartNewTransaction();

        final MutableVertex importVertex = database.newVertex("Import");
        importVertex.set("type", "address conversion");
        importVertex.set("beginTime", beginTime);
        importVertex.set("status", "in progress");
        importVertex.save();

        commitAndStartNewTransaction();

        final Timer timer = new Timer();
        timer.schedule(new TimerTask() {
          @Override
          public void run() {
            printStatus(beginTime, convertedAddresses.get());
          }
        }, DUMP_PROGRESS_EVERY, DUMP_PROGRESS_EVERY);

        try {

          convertTxPart(VERTEX_INPUT_TX);
          convertTxPart(VERTEX_OUTPUT_TX);

          database.async().waitCompletion();

          importVertex.set("status", "completed");
        } catch (Throwable t) {
          importVertex.set("status", "error: " + t.getMessage());
          t.printStackTrace();

        } finally {
          timer.cancel();

          // SAVE THE IMPORT STATUS ALSO IN CASE OF ERROR
          final long endTime = System.currentTimeMillis();
          importVertex.set("endTime", endTime);
          importVertex.set("elapsed", endTime - beginTime);
          importVertex.set("convertedAddresses", convertedAddresses.get());
          importVertex.save();

          System.out.println("Import results:");
          System.out.println(importVertex.toJSON().toString(2));

          database.commit();
        }

      } finally {
        database.close();
      }
    }

  }

  private void convertTxPart(final String type) {
    final ResultSet resultSet = database.query("sql", "select from " + type + " where address is not null");
    for (int i = 0; resultSet.hasNext(); ++i) {
      final Vertex txPartVertex = resultSet.next().getRecord().get().asVertex();

      final String addressHash = txPartVertex.getString("address");

      final Vertex addressVertex = getOrCreateAddress(addressHash);
      txPartVertex.newEdge(BitcoinSchema.EDGE_HAS_ADDRESS, addressVertex, true);

      convertedAddresses.incrementAndGet();

      if (i > 0 && i % commitEvery == 0)
        commitAndStartNewTransaction();
    }
  }

  private void printStatus(final long beginTime, final long convertedAddresses) {
    final long elapsedInSec = (System.currentTimeMillis() - beginTime) / 1000;

    System.out.printf("- converted %,d address (%,d sec) %,d secs elapsed\n",//
        convertedAddresses, convertedAddresses / elapsedInSec, elapsedInSec);
  }

  public static void main(final String[] args) throws IOException {
    new BitcoinAddressConverter().convert();
  }

  private static void printHelp() {
    System.out.println("PLUTOHASH Bitcoin Importer v21.8.1");
    System.out.println("\nSyntax: address-converter.sh [-D<setting>=<value>]*");
    System.out.println("\nSettings:");
    System.out.println("-DcommitEvery=integer (default=100)");
    System.out.println("-DparallelWorkers=integer (default=0 -> auto)");
    System.out.println();
  }
}