package com.plutohub.server;

import com.arcadedb.database.Database;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.utility.FileUtils;
import org.bitcoinj.core.*;
import org.bitcoinj.script.Script;
import org.bitcoinj.script.ScriptPattern;

import java.io.File;
import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;

public class BitcoinDatafileUtils {
  public static final String FIRST_BLOCK_HASH = "0000000000000000000000000000000000000000000000000000000000000000";

  // The method returns a list of files in a directory according to a certain
  // pattern (block files have name blkNNNNN.dat)
  public static List<File> getBitcoinDataFiles(final String bitcoinDataDirectory, final int filesFrom) {
    List<File> list = new LinkedList<File>();
    for (int i = filesFrom; true; i++) {
      File file = new File(bitcoinDataDirectory + String.format(Locale.US, "blk%05d.dat", i));
      if (!file.exists())
        break;
      list.add(file);
    }
    return list;
  }

  public static String blockToStringWithoutTxDetail(final Block block) {
    StringBuilder s = new StringBuilder();
    s.append(" block: \n");
    s.append("   hash: ").append(block.getHashAsString()).append('\n');
    s.append("   version: ").append(block.getVersion());
    String bips = block.isBIP34() ? "BIP34" : "";
    if (block.isBIP66())
      bips += " BIP66";
    if (block.isBIP65())
      bips += " BIP65";
    s.append(" (").append(bips).append(')');
    s.append('\n');
    s.append("   previous block: ").append(block.getPrevBlockHash()).append("\n");
    s.append("   time: ").append(block.getTime()).append(" (").append(Utils.dateTimeFormat(block.getTimeSeconds() * 1000L)).append(")\n");
    s.append("   difficulty target (nBits): ").append(block.getDifficultyTarget()).append("\n");
    s.append("   nonce: ").append(block.getNonce()).append("\n");
    s.append("   transactions: ").append(block.getTransactions() != null ? block.getTransactions().size() : 0).append("\n");

    return s.toString();
  }

  public static void printStatus(final String operation, final List<File> dataFiles, final BlockFileLoader loader, final long beginTime,
      final long loadedBlocks, final long txCounter, final long loadedAddresses, final long linkedAddresses) {
    final int currentFile = loader.getCurrentFileIndex();
    final long totalParsedSize = loader.getTotalParsedSize();
    final long elapsedInSec = (System.currentTimeMillis() - beginTime) / 1000;
    final float progress = currentFile * 100F / dataFiles.size();
    final float extRemainingInSec = (elapsedInSec * dataFiles.size() / currentFile) - elapsedInSec;

    System.out.printf(
        "- %s %,d txs (%,d txs/sec) %,d new addresses %,d addresses links %,d blocks (%,d blocks/sec) %,d/%,d data file parsed %s (%.2f MB/sec) %,d secs elapsed progress=%.2f%% remaining %,d minutes\n",
        operation, txCounter, txCounter / elapsedInSec, loadedAddresses, linkedAddresses, loadedBlocks, loadedBlocks / elapsedInSec, currentFile,
        dataFiles.size(), FileUtils.getSizeAsString(totalParsedSize), totalParsedSize / elapsedInSec / 1024F / 1024F,//
        elapsedInSec, progress, (int) (extRemainingInSec / 60) + 1);
  }

  public static String getAddress(final Script script, final NetworkParameters params) {
    if (ScriptPattern.isP2PKH(script))
      return LegacyAddress.fromPubKeyHash(params, ScriptPattern.extractHashFromP2PKH(script)).toString();
    else if (ScriptPattern.isP2SH(script))
      return LegacyAddress.fromScriptHash(params, ScriptPattern.extractHashFromP2SH(script)).toString();
    else if (ScriptPattern.isP2WH(script))
      return SegwitAddress.fromHash(params, ScriptPattern.extractHashFromP2WH(script)).toString();

    return null;
  }

  public static MutableVertex blockToVertex(final Database database, final Block block, final File originFile, final long originFileOffset) {
    final String hash = block.getHashAsString();
    if (hash == null) {
      System.err.println("Null hash block '" + block + "'");
      return null;
    }

    final MutableVertex persistentBlock = database.newVertex("Block");
    persistentBlock.set("hash", hash); // TODO CHANGE IN ID
    persistentBlock.set("prevBlockHash", block.getPrevBlockHash().toString());
    persistentBlock.set("time", block.getTime().getTime());
    persistentBlock.set("difficultyTarget", block.getDifficultyTarget());
    persistentBlock.set("nonce", block.getNonce());
    persistentBlock.set("timeSeconds", block.getTimeSeconds());
    persistentBlock.set("version", block.getVersion());
    persistentBlock.set("witnessRoot", block.getWitnessRoot().toString());

    persistentBlock.set("originFile", originFile.getName());
    persistentBlock.set("originFileOffset", originFileOffset);

    persistentBlock.set("fullyParsed", false);

    return persistentBlock;
  }

  public static MutableVertex txToVertex(Database database, Transaction tx) {
    final MutableVertex persistentTx = database.newVertex(BitcoinSchema.VERTEX_TRANSACTION);
    persistentTx.set("id", tx.getTxId().toString());
    persistentTx.set("confidence", tx.getConfidence().getConfidenceType().toString());
    persistentTx.set("lockTime", tx.getLockTime());
    persistentTx.set("wtxid", tx.getWTxId().toString());
    persistentTx.set("sigOpCount", tx.getSigOpCount());
    persistentTx.set("version", tx.getVersion());
    persistentTx.set("coinbase", tx.isCoinBase());
    try {
      final Field offset = tx.getClass().getSuperclass().getSuperclass().getDeclaredField("offset");
      offset.setAccessible(true);
      persistentTx.set("originFileOffset", offset.get(tx));
    } catch (Exception e) {
      e.printStackTrace();
    }

    if (tx.getExchangeRate() != null)
      persistentTx.set("exchangeRateCoin", tx.getExchangeRate().coin.value);
    if (tx.getMemo() != null)
      persistentTx.set("memo", tx.getMemo());
    if (tx.getPurpose() != null)
      persistentTx.set("purpose", tx.getPurpose().toString());
    if (tx.getInputSum() != null)
      persistentTx.set("inputSum", tx.getInputSum().value);
    if (tx.getOutputSum() != null)
      persistentTx.set("outputSum", tx.getOutputSum().value);
    if (tx.getFee() != null)
      persistentTx.set("fee", tx.getFee().value);

    return persistentTx;
  }

}