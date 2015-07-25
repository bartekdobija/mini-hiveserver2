package org.apache.hive.jdbc.miniHS2;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class MiniHS2Test {

  MiniHS2 minihs2;

  private static final Path DATA_DIR = new Path("/data");

  @Before
  public void setUp() throws Exception {
    minihs2 = new MiniHS2.Builder().withConf(new HiveConf())
        .withMiniMR().build();
    Map<String, String> config = new HashMap<>();
    minihs2.start(config);
  }

  @After
  public void tearDown() throws Exception {
    minihs2.stop();
  }

  @Test
  public void testMiniHS2() throws Exception {

    assertTrue(minihs2.isStarted());

    FileSystem fs = minihs2.getDfs().getFileSystem();
    // jdbc:hive2://localhost:50142/default
    LoggerFactory.getLogger("JDBC URL: " + getClass()).info(
        minihs2.getJdbcURL());
    fs.mkdirs(DATA_DIR);
    fs.copyFromLocalFile(new Path("src/test/resources/test.csv"), DATA_DIR);
  }

}
