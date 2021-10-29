/*
 * Copyright 2021 Arcade Data Ltd
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.plutohub.server.plugin;

import com.arcadedb.database.Database;
import com.arcadedb.graph.Vertex;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.server.security.ServerSecurityUser;
import com.plutohub.server.BitcoinSchema;
import io.undertow.server.HttpServerExchange;
import org.json.JSONObject;

import java.util.*;

public class GetTransactionHandler extends PlutoHttpHandler {
  public GetTransactionHandler(final BackendPlugin backend) {
    super(backend);
  }

  @Override
  protected void execute(final HttpServerExchange exchange, final ServerSecurityUser user) {
    final Deque<String> idParam = exchange.getQueryParameters().get("id");
    if (idParam == null || idParam.isEmpty()) {
      exchange.setStatusCode(400);
      exchange.getResponseSender().send("{ \"error\" : \"id is null\"}");
      return;
    }

    final Database database = backend.getDatabase();

    database.begin();
    try {
      final IndexCursor cursor = database.lookupByKey(BitcoinSchema.VERTEX_TRANSACTION, "hash", idParam.getFirst());
      if (cursor.hasNext()) {
        final Vertex transaction = cursor.next().asVertex();

        exchange.setStatusCode(200);
        final JSONObject response = new JSONObject().put("result", httpServer.getJsonSerializer().serializeDocument(transaction));
        exchange.getResponseSender().send(response.toString(isCompressOutput(exchange) ? 0 : 2));
      } else {
        exchange.setStatusCode(404);
        exchange.getResponseSender().send("{ \"error\" : \"transaction not found\"}");
      }
    } finally {
      database.rollbackAllNested();
    }
  }
}
