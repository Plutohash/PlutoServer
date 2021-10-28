package com.plutohub.server;

import org.bitcoinj.core.*;
import org.bitcoinj.params.MainNetParams;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

public class BitcoinDatafileScanner {
  // SETTINGS, THE USER CAN OVERRIDE BY SETTING PROPERTIES AT JVM LEVEL
  private       String               bitcoinDataDirectory = "/Volumes/ext/BitCoin/datafiles/blocks/";
  private       int                  expectedTotalBlocks  = 700_000;
  private       int                  parallelWorkers      = 3;
  private       int                  verbose              = 1;
  private final BitcoinImportMetrics metrics              = new BitcoinImportMetrics();

  public BitcoinDatafileScanner(final String bitcoinDataDirectory) {
    this.bitcoinDataDirectory = bitcoinDataDirectory;

    loadSettings();
  }

  private void scan(final int filesFrom, final String[] blocks, final String[] txs, final String[] addresses) {
    System.out.println(
        "1st pass: parsing .dat block files from '" + bitcoinDataDirectory + "' scanning for blocks=" + Arrays.toString(blocks) + " txs=" + Arrays.toString(txs)
            + "...");

    if (blocks.length == 0 && txs.length == 0)
      return;

    // Just some initial setup
    NetworkParameters np = new MainNetParams();
    Context.getOrCreate(MainNetParams.get());

    // We create a BlockFileLoader object by passing a list of files.
    // The list of files is built with the method buildList(), see
    // below for its definition.
    final List<File> dataFiles = BitcoinDatafileUtils.getBitcoinDataFiles(bitcoinDataDirectory, filesFrom);

    //System.out.println("- scanning from the following files: " + dataFiles);

    BlockFileLoader loader = new BlockFileLoader(np, dataFiles);

    final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    if (verbose > 1)
      loader.registerCallback(new BlockFileLoader.OnNewFile() {
        @Override
        public void onNewFile(File prevFile, BitcoinBlock firstBlock, BitcoinBlock lastBlock) {
          System.out.println("- finished file " + prevFile +//
              " blocks " + firstBlock.getOriginBlock().getHashAsString() + " (" + df.format(firstBlock.getOriginBlock().getTimeSeconds() * 1000) + ") - "
              + lastBlock.getOriginBlock().getHashAsString() + "(" + df.format(lastBlock.getOriginBlock().getTimeSeconds() * 1000) + ")");
        }
      });

    final long beginTime = System.currentTimeMillis();

    long lastTxCounterPrinted = 0;

    for (BitcoinBlock b : loader) {
      final long loadedBlocks = metrics.loadBlocksParsedBlocks.incrementAndGet();

      final Block block = b.getOriginBlock();

      if (verbose > 2) {
        System.out.println("- parsed block '" + block.getHashAsString() + "' in file '" + loader.getCurrentFile().getName() + "' date " + df.format(
            block.getTimeSeconds() * 1000));
      }

      for (String blockHash : blocks) {
        if (blockHash.equals(block.getHashAsString())) {
          System.out.println("- found block with hash '" + blockHash + "':\n" + BitcoinDatafileUtils.blockToStringWithoutTxDetail(block));
        }

        if (blockHash.equals(block.getPrevBlockHash().toString())) {
          System.out.println("- found block with previous hash '" + blockHash + "' in file '" + loader.getCurrentFile().getName() + "':\n"
              + BitcoinDatafileUtils.blockToStringWithoutTxDetail(block));
        }
      }

      if (txs.length > 0 || addresses.length > 0) {
        for (Transaction tx : block.getTransactions()) {
          final long txCounter = metrics.loadBlocksParsedTransactions.incrementAndGet();

          for (String txHash : txs) {
            if (txHash.equals(tx.getHash().toString())) {
              System.out.println("- found tx with hash '" + txHash + "':\n" + tx.toString());
            }
          }

          if (addresses.length > 0) {
            if (tx.getInputs() != null && !tx.getInputs().isEmpty()) {
              for (TransactionInput txInput : tx.getInputs()) {
                metrics.loadBlocksNewTransactionInputs.incrementAndGet();
                for (String address : addresses) {

                  final String hexAddress = Utils.HEX.encode(txInput.getScriptSig().getToAddress(tx.getParams()).getHash());
                  if (address.equals(hexAddress))
                    System.out.println(
                        "- found address in transaction '" + tx.getHash().toString() + "' input with seq. number " + txInput.getSequenceNumber() + ":\n"
                            + tx.toString());
                }
              }
            }

            if (tx.getOutputs() != null && !tx.getOutputs().isEmpty()) {
              for (TransactionOutput txOutput : tx.getOutputs()) {
                metrics.loadBlocksNewTransactionInputs.incrementAndGet();
                for (String address : addresses) {

                  final String hexAddress = Utils.HEX.encode(txOutput.getScriptPubKey().getToAddress(tx.getParams()).getHash());
                  if (address.equals(hexAddress))
                    System.out.println(
                        "- found address in transaction '" + tx.getHash().toString() + "' output with index " + txOutput.getIndex() + ":\n" + tx.toString());

                }
              }
            }
          }

          if (verbose > 0 && txCounter > 0 && txCounter % 10_000_000 == 0) {
            BitcoinDatafileUtils.printStatus("scanned", dataFiles, loader, beginTime, loadedBlocks, txCounter, metrics.loadBlocksAddresses.get(),
                metrics.connectAddressesEdges.get());
          }
        }
      } else {
        final long txCounter = metrics.loadBlocksParsedTransactions.addAndGet(block.getTransactions().size());
        if (txCounter - lastTxCounterPrinted > 10_000_000) {
          BitcoinDatafileUtils.printStatus("scanned", dataFiles, loader, beginTime, loadedBlocks, txCounter, metrics.loadBlocksAddresses.get(),
              metrics.connectAddressesEdges.get());
          lastTxCounterPrinted = txCounter;
        }
      }
    }

    System.out.println("1st pass completed in " + ((System.currentTimeMillis() - beginTime) / 1000) + " secs");
  }

  protected void loadSettings() {
    expectedTotalBlocks = Integer.parseInt(System.getProperty("expectedTotalBlocks", "" + expectedTotalBlocks));
    parallelWorkers = Integer.parseInt(System.getProperty("parallelWorkers", "" + parallelWorkers));
    verbose = Integer.parseInt(System.getProperty("verbose", "" + verbose));
  }

  public static void main(final String[] args) throws Exception {
    if (args.length == 0) {
      printHelp();
      return;
    }

    final String bitcoinDataDirectory = args[0];

    final BitcoinDatafileScanner scanner = new BitcoinDatafileScanner(bitcoinDataDirectory);

    int filesFrom = 0;
    String[] blocks = new String[0];
    String[] txs = new String[0];
    String[] addresses = new String[0];

    for (int i = 1; i < args.length; i++) {
      try {
        final String[] param = args[i].split("=");
        final String[] parts = param[1].split(",");

        switch (param[0]) {
        case "filesFrom":
          filesFrom = Integer.parseInt(parts[0]);
          break;
        case "blocks":
          blocks = parts;
          break;
        case "txs":
          txs = parts;
          break;
        case "addresses":
          addresses = parts;
          break;
        }
      } catch (Throwable t) {
        System.err.println("Error on parsing input parameter '" + args[i] + "': " + t.getMessage());
      }
    }

    scanner.scan(filesFrom, blocks, txs, addresses);
  }

  private static void printHelp() {
    System.out.println("PLUTOHASH Bitcoin Scanner v21.8.1");
    System.out.println(
        "\nSyntax: scanner.sh [-D<setting>=<value>]* <bitcoinDataDirectory> [filesFrom=<file number>] [blocks=<hash[,]*>] [txs=<hash[,]*>] [addresses=<hash[,]*>]");
    System.out.println("\nSettings:");
    System.out.println("-DexpectedTotalBlocks=integer (default=700000)");
    System.out.println("-DparallelWorkers=integer (default=3)");
    System.out.println("-Dverbose=integer (default=0)");
    System.out.println();
  }
}