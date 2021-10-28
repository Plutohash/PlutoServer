package com.plutohub.server;

import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.graph.Edge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.script.ScriptException;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;

public class BitcoinImporter extends BitcoinJob {
  private static final int DUMP_PROGRESS_EVERY = 5_000;

  // SETTINGS, THE USER CAN OVERRIDE BY SETTING PROPERTIES AT JVM LEVEL
  private       String               bitcoinDataDirectory = "/Volumes/ext/BitCoin/datafiles/blocks/";
  private       boolean              dropExistentDatabase = false;
  private       boolean              addressAsVertex      = false;
  private       int                  safeBlocksFromTail   = 6; // LAST BLOCK FROM THE TAIL CONSIDERED TO BE FINAL
  private       int                  expectedTotalBlocks  = 700_000;
  private       int                  limitBlocks          = 0;
  private       int                  limitTransactions    = 0;
  private final BitcoinImportMetrics metrics              = new BitcoinImportMetrics();
  private final BitcoinSchema        schema               = new BitcoinSchema();

  // A simple method with everything in it
  public void load(final String bitcoinDataDirectory) {
    this.bitcoinDataDirectory = bitcoinDataDirectory;

    loadSettings();

    try (DatabaseFactory factory = new DatabaseFactory("bitcoin")) {
      if (dropExistentDatabase && factory.exists())
        factory.open().drop();

      if (factory.exists())
        database = factory.open();
      else
        database = factory.create();

      try {
        schema.sync(database);

        final long beginTime = System.currentTimeMillis();

        database.begin();
        database.getTransaction().setUseWAL(false);

        final MutableVertex importVertex = database.newVertex("Import");
        importVertex.set("beginTime", beginTime);
        importVertex.set("status", "in progress");

        try {
          loadBlocks();
          connectBlocks(importVertex);

          database.async().waitCompletion();

          importVertex.set("status", "completed");
        } catch (Throwable t) {
          importVertex.set("status", "error: " + t.getMessage());
          t.printStackTrace();

        } finally {
          // SAVE THE IMPORT STATUS ALSO IN CASE OF ERROR
          final long endTime = System.currentTimeMillis();
          importVertex.set("endTime", endTime);
          importVertex.set("elapsed", endTime - beginTime);
          importVertex.set("parsedBlocks", metrics.loadBlocksParsedBlocks.get());
          importVertex.set("parsedTransactions", metrics.loadBlocksParsedTransactions.get());
          importVertex.set("newAddresses", metrics.loadBlocksAddresses.get());
          importVertex.set("newBlocks", metrics.loadBlocksNewBlocks.get());
          importVertex.set("newTransactions", metrics.loadBlocksNewTransactions.get());
          importVertex.set("newTransactionInputs", metrics.loadBlocksNewTransactionInputs.get());
          importVertex.set("hashNotFound", metrics.connectBlocksNotFound.size());
          importVertex.save();

          if (metrics.loadBlocksHeadBlock != null)
            importVertex.newLightEdge(BitcoinSchema.EDGE_HEAD_BLOCK, metrics.loadBlocksHeadBlock, false);
          if (metrics.loadBlocksTailBlock != null) {
            importVertex.newLightEdge(BitcoinSchema.EDGE_TAIL_BLOCK, metrics.loadBlocksTailBlock, false);

            // FROM THE TAIL GO BACK TO GET A SAFE NODE
            Vertex latestSafeBlock = ((Vertex) metrics.loadBlocksTailBlock.getRecord());

            for (int i = 0; i < safeBlocksFromTail; i++) {
              final Iterator<Edge> prevCursor = latestSafeBlock.getEdges(Vertex.DIRECTION.OUT, BitcoinSchema.EDGE_PREVIOUS_BLOCK).iterator();
              if (!prevCursor.hasNext())
                break;

              latestSafeBlock = (Vertex) prevCursor.next().getRecord();
            }

            importVertex.newLightEdge(BitcoinSchema.EDGE_SAFE_TAIL_BLOCK, latestSafeBlock, false);
          }

          if (!metrics.connectBlocksNotFound.isEmpty()) {
            System.out.println("The following blocks were not found to create the blockchain:");
            for (Map.Entry<String, String> entry : metrics.connectBlocksNotFound.entrySet()) {
              System.out.println("- " + entry.getKey() + " as previous block for " + entry.getValue());
            }
          }

          System.out.println("Import results:");
          System.out.println(importVertex.toJSON().toString(2));

          database.commit();
        }

      } finally {
        database.close();
      }
    }

  }

  private void loadBlocks() {
    System.out.println("Parsing .dat block files and create the graph database (commitEvery=" + commitEvery + ")...");

    // Just some initial setup
    NetworkParameters np = new MainNetParams();
    Context.getOrCreate(MainNetParams.get());

    final List<File> dataFiles = BitcoinDatafileUtils.getBitcoinDataFiles(bitcoinDataDirectory, 0);
    BlockFileLoader loader = new BlockFileLoader(np, dataFiles);

    final long beginTime = System.currentTimeMillis();

    if (!addressAsVertex)
      prepareAsync();

    final Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        BitcoinDatafileUtils.printStatus("imported", dataFiles, loader, beginTime, metrics.loadBlocksParsedBlocks.get(),
            metrics.loadBlocksParsedTransactions.get(), metrics.loadBlocksAddresses.get(), metrics.connectAddressesEdges.get());
      }
    }, DUMP_PROGRESS_EVERY, DUMP_PROGRESS_EVERY);

    try {
      for (BitcoinBlock b : loader) {
        final Block block = b.getOriginBlock();

        final long loadedBlocks = metrics.loadBlocksParsedBlocks.incrementAndGet();

        final MutableVertex persistentBlock = BitcoinDatafileUtils.blockToVertex(database, block, b.getOriginFile(), b.getOriginFileOffset());

        if (persistentBlock == null)
          continue;

        if (block.getPrevBlockHash().toString().equals(BitcoinDatafileUtils.FIRST_BLOCK_HASH))
          // 1ST BLOCK, NO PREVIOUS
          metrics.loadBlocksHeadBlock = persistentBlock.getIdentity();

        if (block.getTimeSeconds() > metrics.loadBlocksLastBlockTimestamp) {
          metrics.loadBlocksTailBlock = persistentBlock.getIdentity();
          metrics.loadBlocksLastBlockTimestamp = block.getTimeSeconds() * 1000L;
        }

        metrics.loadBlocksNewBlocks.incrementAndGet();

        if (addressAsVertex) {
          persistentBlock.save();
          parseTransactions(block, persistentBlock);
          persistentBlock.set("fullyParsed", true).save();
        } else {
          database.async().createRecord(persistentBlock, (savedRecord) -> {
            final MutableVertex saved = savedRecord.asVertex().modify();
            parseTransactions(block, saved);
            saved.modify().set("fullyParsed", true).save();
          });
        }

        if (limitTransactions > 0 && metrics.loadBlocksNewTransactions.get() > limitTransactions)
          break;

        if (limitBlocks > 0 && loadedBlocks > limitBlocks) {
          System.out.println("- reached limit of " + limitBlocks + " blocks");
          break;
        }
      }
    } finally {
      timer.cancel();
    }

    if (addressAsVertex)
      commitAndStartNewTransaction();
    else
      database.async().waitCompletion();

    System.out.println("- completed in " + ((System.currentTimeMillis() - beginTime) / 1000) + " secs");
  }

  private void parseTransactions(final Block block, final MutableVertex persistentBlock) {
    for (Transaction tx : block.getTransactions()) {
      final MutableVertex persistentTransaction = BitcoinDatafileUtils.txToVertex(database, tx);
      persistentTransaction.save();

      metrics.loadBlocksNewTransactions.incrementAndGet();

      persistentBlock.newLightEdge(BitcoinSchema.EDGE_BLOCK_TX, persistentTransaction, true);

      if (tx.getInputs() != null && !tx.getInputs().isEmpty()) {
        for (TransactionInput txInput : tx.getInputs()) {
          try {
            parseTxInput(persistentTransaction, txInput);
            metrics.loadBlocksNewTransactionInputs.incrementAndGet();
          } catch (IOException e) {
            e.printStackTrace();
            break;
          }
        }
      }

      if (tx.getOutputs() != null && !tx.getOutputs().isEmpty()) {
        for (TransactionOutput txOutput : tx.getOutputs()) {
          try {
            parseTxOutput(persistentTransaction, txOutput);
            metrics.loadBlocksNewTransactionOutputs.incrementAndGet();
          } catch (IOException e) {
            e.printStackTrace();
            break;
          }
        }
      }

      final long txCounter = metrics.loadBlocksParsedTransactions.incrementAndGet();

      if (addressAsVertex) {
        if (txCounter > 0 && txCounter % commitEvery == 0)
          commitAndStartNewTransaction();
      }

      if (limitTransactions > 0 && metrics.loadBlocksNewTransactions.get() > limitTransactions) {
        System.out.println("- reached limit of " + limitTransactions + " transactions");
        break;
      }
    }
  }

  private void connectBlocks(MutableVertex importVertex) {
    System.out.println("Build the block chain...");

    final long beginTime = System.currentTimeMillis();

    database.scanType(BitcoinSchema.VERTEX_BLOCK, false, (record) -> {
      final Vertex block = (Vertex) record;
      final long loaded = metrics.connectBlocksLoadedBlocks.incrementAndGet();

      if (loaded % 100_000 == 0) {
        System.out.println(
            "- elapsed " + ((System.currentTimeMillis() - beginTime) / 1000) + " secs loaded: " + loaded + " okLinks: " + metrics.connectBlocksOkLinks
                + " koLinks: " + metrics.connectBlocksKoLinks + " skipped: " + metrics.connectBlocksSkipped);
      }

      final String blockHash = block.getString("hash");

      if (block.getEdges(Vertex.DIRECTION.OUT, BitcoinSchema.EDGE_PREVIOUS_BLOCK).iterator().hasNext()) {
        //System.out.println("- Skip block with hash '" + block.get("hash") + "' because already connected");
        metrics.connectBlocksSkipped.incrementAndGet();
        return true;
      }

      final String prevBlockHash = block.getString("prevBlockHash");

      if (prevBlockHash.equals(BitcoinDatafileUtils.FIRST_BLOCK_HASH))
        return true;

      if (prevBlockHash == null) {
        System.out.println("- error: cannot find prevBlockHash in block with hash '" + blockHash + "'");
        return true;
      }

      final IndexCursor previousBlockCursor = database.lookupByKey(BitcoinSchema.VERTEX_BLOCK, new String[] { "hash" }, new Object[] { prevBlockHash });
      if (!previousBlockCursor.hasNext()) {
        metrics.connectBlocksNotFound.put(prevBlockHash, blockHash);
        System.out.println("- error: block with hash '" + prevBlockHash + "' not found in database");
        metrics.connectBlocksKoLinks.incrementAndGet();
        return true;
      }

      final Vertex previousBlock = (Vertex) previousBlockCursor.next().getRecord();

      block.newLightEdge(BitcoinSchema.EDGE_PREVIOUS_BLOCK, previousBlock, true);

      metrics.connectBlocksOkLinks.incrementAndGet();

      return true;
    });

    commitAndStartNewTransaction();

    final long endTime = System.currentTimeMillis();

    System.out.println("- completed in " + ((endTime - beginTime) / 1000) + " secs loaded: " + metrics.connectBlocksLoadedBlocks.get() + " okLinks: "
        + metrics.connectBlocksOkLinks + " koLinks: " + metrics.connectBlocksKoLinks + " skipped: " + metrics.connectBlocksSkipped);
  }

  protected void parseTxInput(final Vertex transaction, final TransactionInput txInput) throws IOException {
    try {
      final Field offset = txInput.getClass().getSuperclass().getSuperclass().getDeclaredField("offset");
      offset.setAccessible(true);

      final String hexAddress = txInput.isCoinBase() ? null : BitcoinDatafileUtils.getAddress(txInput.getScriptSig(), txInput.getParams());

      final MutableVertex txInputVertex = database.newVertex(BitcoinSchema.VERTEX_INPUT_TX);
      txInputVertex.set("sourceOffset", (Integer) offset.get(txInput));
      txInputVertex.set("index", txInput.getIndex());
      txInputVertex.set("value", txInput.getValue() != null ? txInput.getValue().value : 0);

      if (!addressAsVertex && hexAddress != null)
        txInputVertex.set("address", hexAddress);

      txInputVertex.save();

      transaction.newLightEdge(BitcoinSchema.EDGE_INPUT, txInputVertex, true);

      if (txInput.isCoinBase())
        return;

      if (hexAddress == null)
        return;

      if (addressAsVertex) {
        final Vertex address = getOrCreateAddress(hexAddress);

        txInputVertex.newLightEdge(BitcoinSchema.EDGE_HAS_ADDRESS, address, true);

        metrics.connectAddressesEdges.incrementAndGet();
      }

    } catch (ScriptException e) {
      metrics.errorParsingAddresses.incrementAndGet();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  protected void parseTxOutput(final Vertex transaction, final TransactionOutput txOutput) throws IOException {
    try {
      final Field offset = txOutput.getClass().getSuperclass().getSuperclass().getDeclaredField("offset");
      offset.setAccessible(true);

      final String hexAddress = BitcoinDatafileUtils.getAddress(txOutput.getScriptPubKey(), txOutput.getParams());

      final MutableVertex txOutputVertex = database.newVertex(BitcoinSchema.VERTEX_INPUT_TX);
      txOutputVertex.set("sourceOffset", (Integer) offset.get(txOutput));
      txOutputVertex.set("index", txOutput.getIndex());
      txOutputVertex.set("value", txOutput.getValue() != null ? txOutput.getValue().value : 0);

      if (!addressAsVertex && hexAddress != null)
        txOutputVertex.set("address", hexAddress);

      txOutputVertex.save();

      transaction.newLightEdge(BitcoinSchema.EDGE_OUTPUT, txOutputVertex, true);

      if (hexAddress == null)
        return;

      if (addressAsVertex) {
        final Vertex address = getOrCreateAddress(hexAddress);

        txOutputVertex.newLightEdge(BitcoinSchema.EDGE_HAS_ADDRESS, address, true);

        metrics.connectAddressesEdges.incrementAndGet();
      }

    } catch (ScriptException e) {
      metrics.errorParsingAddresses.incrementAndGet();
    } catch (Exception e2) {
      e2.printStackTrace();
    }
  }

  @Override
  protected void loadSettings() {
    super.loadSettings();
    dropExistentDatabase = Boolean.parseBoolean(System.getProperty("dropExistentDatabase", "false"));
    safeBlocksFromTail = Integer.parseInt(System.getProperty("safeBlocksFromTail", "" + safeBlocksFromTail));
    expectedTotalBlocks = Integer.parseInt(System.getProperty("expectedTotalBlocks", "" + expectedTotalBlocks));
    limitBlocks = Integer.parseInt(System.getProperty("limitBlocks", "" + limitBlocks));
    limitTransactions = Integer.parseInt(System.getProperty("limitTransactions", "" + limitTransactions));
    addressAsVertex = Boolean.parseBoolean(System.getProperty("addressAsVertex", "false"));
  }

  public static void main(final String[] args) throws IOException {
    if (args.length == 0) {
      printHelp();
      return;
    }

    final BitcoinImporter tb = new BitcoinImporter();

    final String bitcoinDataDirectory = args[0];

    tb.load(bitcoinDataDirectory);

    if (args.length > 1) {
      final BitcoinAddressLabelImporter addressLabelImporter = new BitcoinAddressLabelImporter();
      addressLabelImporter.parseCSV(args[1]);
    }
  }

  private static void printHelp() {
    System.out.println("PLUTOHASH Bitcoin Importer v21.8.1");
    System.out.println("\nSyntax: importer.sh [-D<setting>=<value>]* <bitcoinDataDirectory> [<csv-address-file>]");
    System.out.println("\nSettings:");
    System.out.println("-DdropExistentDatabase=true|false (default=false)");
    System.out.println("-DcommitEvery=integer (default=100)");
    System.out.println("-DaddressAsVertex=true|false (default=false)");
    System.out.println("-DsafeBlocksFromTail=integer (default=6)");
    System.out.println("-DexpectedTotalBlocks=integer (default=700000)");
    System.out.println("-DparallelWorkers=integer (default=0 -> auto)");
    System.out.println("-DlimitBlocks=integer (default=0 - unlimited)");
    System.out.println();
  }
}