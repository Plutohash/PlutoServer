package com.plutohub.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Identifiable;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SQLEngine;
import com.arcadedb.query.sql.function.SQLFunctionAbstract;
import org.bitcoinj.script.Script;

import java.io.IOException;

public class BitcoinConsole {
  public void console(final String language, final String command) throws IOException {

    final String databasePath = System.getProperty("pluto.database", "databases/bitcoin");

    try (DatabaseFactory factory = new DatabaseFactory(databasePath)) {
      final Database database = factory.open();

      SQLEngine.getInstance().getFunctionFactory().register(new SQLFunctionAbstract("bitcoinHex2Script") {
        @Override
        public Object execute(Object iThis, Identifiable iCurrentRecord, Object iCurrentResult, Object[] iParams, CommandContext iContext) {
          return new Script((byte[]) iCurrentResult).toString();
        }

        @Override
        public String getSyntax() {
          return "Converts byte[] in hex format into bitcoin script language";
        }
      });

      final long beginTime = System.currentTimeMillis();
      database.begin();
      try {

        for (final ResultSet resultset = database.command(language, command); resultset.hasNext(); ) {
          System.out.println(resultset.next().toJSON());
        }

        database.commit();

        final float elapsed = (System.currentTimeMillis() - beginTime) / 1000F;

        System.out.printf("Elapsed %,.2f seconds\n", elapsed);

      } finally {
        database.close();
      }
    }
  }  // end of doSomething() method.

  public static void main(final String[] args) throws IOException {
    if (args.length < 2) {
      printHelp();
      return;
    }

    final BitcoinConsole tb = new BitcoinConsole();

    String command = "";
    for (int i = 1; i < args.length; i++) {
      if (i > 1)
        command += " ";
      command += args[i];
    }

    System.out.println(
        "NOTE: This console opens and closes the database as embedded at every command. In order to have better performance, use a database behind an ArcadeDB Server.");

    tb.console(args[0], command);
  }

  private static void printHelp() {
    System.out.println("PLUTOHASH Bitcoin Console v21.8.1");
    System.out.println("\nSyntax: console.sh <language> <command>");
    System.out.println();
  }
}