package com.plutohub.server;

import com.arcadedb.database.Database;
import com.arcadedb.database.async.ErrorCallback;
import com.arcadedb.engine.WALFile;

public class DatabaseJob {
  protected Database database        = null;
  protected int      commitEvery     = 100;
  protected int      parallelWorkers = 0;

  protected void prepareAsync() {
    if (parallelWorkers > 0)
      database.async().setParallelLevel(parallelWorkers);
    database.async().setTransactionUseWAL(false);
    database.async().setTransactionSync(WALFile.FLUSH_TYPE.NO);
    database.async().setCommitEvery(commitEvery);
    database.async().onError(new ErrorCallback() {
      @Override
      public void call(Throwable exception) {
        System.err.println("CRITICAL ERROR: " + exception);
        exception.printStackTrace();
      }
    });
  }

  protected void commitAndStartNewTransaction() {
    if (database.isTransactionActive())
      database.commit();

    database.begin();
    database.setUseWAL(false);
  }

  protected void loadSettings() {
    commitEvery = Integer.parseInt(System.getProperty("commitEvery", "" + commitEvery));
    parallelWorkers = Integer.parseInt(System.getProperty("parallelWorkers", "" + parallelWorkers));
  }
}