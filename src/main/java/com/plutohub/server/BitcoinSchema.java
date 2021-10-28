package com.plutohub.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.bucketselectionstrategy.PartitionedBucketSelectionStrategy;
import com.arcadedb.schema.Property;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

public class BitcoinSchema {
  public static final String VERTEX_BLOCK         = "Block";
  public static final String VERTEX_IMPORT        = "Import";
  public static final String VERTEX_ADDRESS       = "Address";
  public static final String VERTEX_INPUT_TX      = "InputTx";
  public static final String VERTEX_OUTPUT_TX     = "OutputTx";
  public static final String EDGE_HAS_ADDRESS     = "HasAddress"; // TODO: SEPARATE IN 2 EDGES SO IT'S FASTER TO RETRIEVE IN AND OUT TX WIHTOUT FILTERING
  public static final String EDGE_PREVIOUS_BLOCK  = "PreviousBlock";
  public static final String EDGE_HEAD_BLOCK      = "HeadBlock";
  public static final String EDGE_TAIL_BLOCK      = "TailBlock";
  public static final String EDGE_SAFE_TAIL_BLOCK = "SafeTailBlock";
  public static final String EDGE_BLOCK_TX        = "BlockTx";
  public static final String EDGE_INPUT           = "Input";
  public static final String EDGE_OUTPUT          = "Output";
  public static final String VERTEX_TRANSACTION   = "Transaction";

  public void sync(final Database database) {
    database.getSchema().getOrCreateVertexType(VERTEX_IMPORT);

    final VertexType addressType = database.getSchema().getOrCreateVertexType(VERTEX_ADDRESS);
    final Property addressHash = addressType.getOrCreateProperty("hash", Type.STRING); // TODO: RENAME IN ID
    database.transaction((db) -> {
      addressHash.getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    });

    final VertexType tx = database.getSchema().getOrCreateVertexType(VERTEX_TRANSACTION);
    final Property txId = tx.getOrCreateProperty("id", Type.STRING);
    database.transaction((db) -> {
      txId.getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    });

    final VertexType blockType = database.getSchema().getOrCreateVertexType(VERTEX_BLOCK);
    final Property blockHash = blockType.getOrCreateProperty("hash", Type.STRING);
    database.transaction((db) -> {
      blockHash.getOrCreateIndex(Schema.INDEX_TYPE.LSM_TREE, true);
    });
    blockType.setBucketSelectionStrategy(new PartitionedBucketSelectionStrategy(new String[] { "hash" }));

    database.getSchema().getOrCreateVertexType(VERTEX_INPUT_TX);
    database.getSchema().getOrCreateVertexType(VERTEX_OUTPUT_TX);

    database.getSchema().getOrCreateEdgeType(EDGE_HAS_ADDRESS);

    database.getSchema().getOrCreateEdgeType(EDGE_PREVIOUS_BLOCK);
    database.getSchema().getOrCreateEdgeType(EDGE_BLOCK_TX);
    database.getSchema().getOrCreateEdgeType(EDGE_INPUT);
    database.getSchema().getOrCreateEdgeType(EDGE_OUTPUT);
    database.getSchema().getOrCreateEdgeType(EDGE_HEAD_BLOCK);
    database.getSchema().getOrCreateEdgeType(EDGE_TAIL_BLOCK);
    database.getSchema().getOrCreateEdgeType(EDGE_SAFE_TAIL_BLOCK);
  }
}