package com.plutohub.server;

import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;

public class BitcoinJob extends DatabaseJob {
  protected Vertex getOrCreateAddress(final String hexAddress) {
    final IndexCursor cursor = database.lookupByKey(BitcoinSchema.VERTEX_ADDRESS, "hash", hexAddress);

    final Vertex address;
    if (cursor.hasNext()) {
      address = cursor.next().asVertex();
    } else {
      address = database.newVertex(BitcoinSchema.VERTEX_ADDRESS).set("hash", hexAddress).save();
    }
    return address;
  }
}