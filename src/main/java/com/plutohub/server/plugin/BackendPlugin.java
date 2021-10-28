package com.plutohub.server.plugin;

import com.arcadedb.ContextConfiguration;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.engine.PaginatedFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.server.ArcadeDBServer;
import com.arcadedb.server.ServerPlugin;
import com.arcadedb.server.http.HttpServer;
import io.undertow.Handlers;
import io.undertow.server.handlers.PathHandler;

import java.util.logging.Level;

public class BackendPlugin implements ServerPlugin {
  private ArcadeDBServer       server;
  private ContextConfiguration configuration;
  private Database             database;
  private HttpServer           httpServer;

  @Override
  public void configure(final ArcadeDBServer arcadeDBServer, final ContextConfiguration configuration) {
    this.server = arcadeDBServer;
    this.configuration = configuration;
    this.httpServer = server.getHttpServer();
  }

  @Override
  public void startService() {
    final String databasePath = System.getProperty("pluto.database", "databases/bitcoin");

    database = new DatabaseFactory(databasePath).open(PaginatedFile.MODE.READ_ONLY);

    LogManager.instance().log(this, Level.INFO, "Opened database '%s'", null, databasePath);
  }

  @Override
  public void stopService() {
    LogManager.instance().log(this, Level.INFO, "Closing database '%s'...", null, database.getName());
    database.close();
  }

  @Override
  public void registerAPI(final HttpServer httpServer, final PathHandler routes) {
    // NO AUTHENTICATION
    routes.addPrefixPath("/pluto",//
        Handlers.routing()//
            .get("/block/{id}", new GetBlockHandler(this))//
            .get("/transaction/{id}", new GetTransactionHandler(this))//
            .get("/address/{id}", new GetAddressHandler(this))//
            .get("/addresses", new GetAddressesHandler(this))//
    );
  }

  public Database getDatabase() {
    return database;
  }

  public HttpServer getHttpServer() {
    return httpServer;
  }

  public static void main(String[] args) {
  }
}
