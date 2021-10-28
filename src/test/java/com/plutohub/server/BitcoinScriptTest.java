package com.plutohub.server;

import com.google.common.io.BaseEncoding;
import org.bitcoinj.script.Script;

/**
 * @author Luca Garulli (l.garulli@arcadedata.com)
 **/
public class BitcoinScriptTest {
  public static void main(String[] args) {
    System.out.println(new Script(BaseEncoding.base16().lowerCase().decode("6a4c0a0102030405060708090a")));
  }
}
