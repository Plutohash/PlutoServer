package com.plutohub.server;

import org.bitcoinj.core.Block;

import java.io.File;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public class BitcoinBlock {
  private final File  originFile;
  private final long  originFileOffset;
  private final Block originBlock;

  public BitcoinBlock(final File originFile, final long originFileOffset, final Block originBlock) {
    this.originFile = originFile;
    this.originFileOffset = originFileOffset;
    this.originBlock = originBlock;
  }

  public Block getOriginBlock() {
    return originBlock;
  }

  public File getOriginFile() {
    return originFile;
  }

  public long getOriginFileOffset() {
    return originFileOffset;
  }
}
