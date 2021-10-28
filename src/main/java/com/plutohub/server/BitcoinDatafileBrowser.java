package com.plutohub.server;

import org.bitcoinj.core.Block;
import org.bitcoinj.core.Context;
import org.bitcoinj.core.NetworkParameters;
import org.bitcoinj.core.Transaction;
import org.bitcoinj.params.MainNetParams;
import org.bitcoinj.utils.BlockFileLoader;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.*;

public class BitcoinDatafileBrowser {

  private final static int PARALLEL = 3;

  // Location of block files. This is where your blocks are located.
  // Check the documentation of Bitcoin Core if you are using
  // it, or use any other directory with blk*dat files.
  static String PREFIX = "/Volumes/ext/BitCoin/datafiles/blocks/";

  // A simple method with everything in it
  public void browse() {

    // Just some initial setup
    NetworkParameters np = new MainNetParams();
    Context.getOrCreate(MainNetParams.get());

    // We create a BlockFileLoader object by passing a list of files.
    // The list of files is built with the method buildList(), see
    // below for its definition.
    BlockFileLoader loader = new BlockFileLoader(np, buildList());

    // We are going to store the results in a map of the form
    // day -> n. of transactions
    Map<String, Integer> dailyTotTxs = new HashMap<>();

    // A simple counter to have an idea of the progress
    int blockCounter = 0;

    // bitcoinj does all the magic: from the list of files in the loader
    // it builds a list of blocks. We iterate over it using the following
    // for loop
    for (Block block : loader) {

      blockCounter++;
      // This gives you an idea of the progress
      System.out.println("Analysing block " + blockCounter);

      // Extract the day from the block: we are only interested
      // in the day, not in the time. Block.getTime() returns
      // a Date, which is here converted to a string.
      String day = new SimpleDateFormat("yyyy-MM-dd").format(block.getTime());

      // Now we start populating the map day -> number of transactions.
      // Is this the first time we see the date? If yes, create an entry
      if (!dailyTotTxs.containsKey(day)) {
        dailyTotTxs.put(day, 0);
      }

      // The following is highly inefficient: we could simply do
      // block.getTransactions().size(), but is shows you
      // how to iterate over transactions in a block
      // So, we simply iterate over all transactions in the
      // block and for each of them we add 1 to the corresponding
      // entry in the map

      System.out.println("BLOCK " + block.toString());

      for (Transaction tx : block.getTransactions()) {
        dailyTotTxs.put(day, dailyTotTxs.get(day) + 1);
        System.out.println(tx.toString());
      }

      System.out.println("TX: " + block.getTransactions().size());
      System.out.println("------------------------------------------------------------");

    } // End of iteration over blocks

    // Finally, let's print the results
    for (String d : dailyTotTxs.keySet()) {
      System.out.println(d + "," + dailyTotTxs.get(d));
    }
  }  // end of doSomething() method.

  // The method returns a list of files in a directory according to a certain
  // pattern (block files have name blkNNNNN.dat)
  private List<File> buildList() {
    List<File> list = new LinkedList<File>();
    for (int i = 0; true; i++) {
      File file = new File(PREFIX + String.format(Locale.US, "blk%05d.dat", i));
      if (!file.exists())
        break;
      list.add(file);
    }
    return list;
  }

  private List<File> buildList(int blockNumber) {
    List<File> list = new LinkedList<File>();
    File file = new File(PREFIX + String.format(Locale.US, "blk%05d.dat", blockNumber));
    list.add(file);
    return list;
  }

  // Main method: simply invoke everything
  public static void main(String[] args) {
    BitcoinDatafileBrowser tb = new BitcoinDatafileBrowser();
    //tb.doSomething();
    tb.browse();
  }

}