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
import com.arcadedb.query.sql.executor.ResultSet;
import com.plutohub.server.BitcoinSchema;
import io.undertow.server.HttpServerExchange;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.Deque;
import java.util.Iterator;

public class GetAddressesHandler extends PlutoHttpHandler {
  public GetAddressesHandler(final BackendPlugin backend) {
    super(backend);
  }

  @Override
  protected void execute(final HttpServerExchange exchange) throws Exception {
    final Deque<String> labelParam = exchange.getQueryParameters().get("label");
    final String label = labelParam == null || labelParam.isEmpty() ? null : labelParam.getFirst();

    final Deque<String> limitParam = exchange.getQueryParameters().get("limit");
    final int limit = limitParam == null || limitParam.isEmpty() ? 100 : Integer.parseInt(limitParam.getFirst());

    final int timeout = 10_000;

    final Database database = backend.getDatabase();

    database.begin();
    try {
      final ResultSet cursor;

      if (label != null)
        cursor = database.query("sql", "select from " + BitcoinSchema.VERTEX_ADDRESS + " where label = ? limit " + limit + " timeout " + timeout, label);
      else
        cursor = database.query("sql", "select from " + BitcoinSchema.VERTEX_ADDRESS + " limit " + limit + " timeout " + timeout);

      if (cursor.hasNext()) {
        final JSONObject ret = new JSONObject();

        final JSONArray result = new JSONArray();
        ret.put("result", result);

        while (cursor.hasNext()) {
          final Vertex address = cursor.next().getVertex().get();

          final JSONObject addressJson = httpServer.getJsonSerializer().serializeRecord(address);
          result.put(addressJson);

          final JSONArray inputs = new JSONArray();
          addressJson.put("inputs", inputs);
          final JSONArray outputs = new JSONArray();
          addressJson.put("outputs", outputs);

          // TODO: USER HASINADDRESS AND HASOUTADDRESS
          for (Vertex connectedTx : address.getVertices(Vertex.DIRECTION.IN, "HasAddress")) {
            final JSONObject tx = new JSONObject();
            tx.put("value", connectedTx.getLong("value"));

            Iterator<Vertex> transaction;
            switch (connectedTx.getTypeName()) {
            case "InputTx":
              inputs.put(tx);
              break;
            case "OutputTx":
              outputs.put(tx);
              break;
            }

            transaction = connectedTx.getVertices(Vertex.DIRECTION.IN).iterator();
            if (transaction.hasNext()) {
              final Vertex transactionVertex = transaction.next();
              tx.put("id", transactionVertex.getString("id"));
              tx.put("purpose", transactionVertex.getString("purpose"));
              tx.put("confidence", transactionVertex.getString("confidence"));
              tx.put("inputSum", transactionVertex.getLong("inputSum"));
              tx.put("outputSum", transactionVertex.getLong("outputSum"));
              tx.put("coinbase", transactionVertex.getBoolean("coinbase"));
              tx.put("lockTime", transactionVertex.getLong("lockTime"));
              tx.put("wtxid", transactionVertex.getString("wtxid"));
            }
          }
        }

        exchange.setStatusCode(200);
        exchange.getResponseSender().send(ret.toString(isCompressOutput(exchange) ? 0 : 2));
      } else {
        exchange.setStatusCode(404);
        exchange.getResponseSender().send("{ \"error\" : \"addresses not found\"}");
      }
    } finally {
      database.rollbackAllNested();
    }
  }

}
