package com.plutohub.server;

import com.arcadedb.database.RID;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class BitcoinImportMetrics {
  public final AtomicLong                        loadBlocksParsedBlocks          = new AtomicLong();
  public final AtomicLong                        loadBlocksParsedTransactions    = new AtomicLong();
  public final AtomicLong                        loadBlocksNewBlocks             = new AtomicLong();
  public final AtomicLong                        loadBlocksNewTransactions       = new AtomicLong();
  public final AtomicLong                        loadBlocksNewTransactionInputs  = new AtomicLong();
  public final AtomicLong                        loadBlocksNewTransactionOutputs = new AtomicLong();
  public       RID                               loadBlocksHeadBlock             = null;
  public       RID                               loadBlocksTailBlock             = null;
  public       long                              loadBlocksLastBlockTimestamp    = 0L;
  public       AtomicLong                        loadBlocksAddresses             = new AtomicLong();
  public       AtomicLong                        errorParsingAddresses           = new AtomicLong();
  public final AtomicLong                        connectBlocksLoadedBlocks       = new AtomicLong();
  public final AtomicLong                        connectBlocksSkipped            = new AtomicLong();
  public final AtomicLong                        connectBlocksOkLinks            = new AtomicLong();
  public final AtomicLong                        connectBlocksKoLinks            = new AtomicLong();
  public final ConcurrentHashMap<String, String> connectBlocksNotFound           = new ConcurrentHashMap<>();
  public       AtomicLong                        connectAddressesEdges           = new AtomicLong();
}