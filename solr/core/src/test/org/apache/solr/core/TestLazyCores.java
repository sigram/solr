/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.solr.core;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Pattern;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.StreamingResponseCallback;
import org.apache.solr.client.solrj.embedded.EmbeddedSolrServer;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.CoreAdminParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.handler.admin.CoreAdminHandler;
import org.apache.solr.handler.admin.MetricsHandler;
import org.apache.solr.request.LocalSolrQueryRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.CommitUpdateCommand;
import org.apache.solr.update.UpdateHandler;
import org.apache.solr.util.LogListener;
import org.apache.solr.util.ReadOnlyCoresLocator;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestLazyCores extends SolrTestCaseJ4 {

  /** Transient core cache max size defined in the test solr-transientCores.xml */
  private static final int TRANSIENT_CORE_CACHE_MAX_SIZE = 4;

  private Path solrHomeDirectory;

  @BeforeClass
  public static void setupClass() throws Exception {
    // Need to use a disk-based directory because there are tests that close a core after adding
    // documents then expect to be able to re-open that core and execute a search
    useFactory("solr.StandardDirectoryFactory");
  }

  private static CoreDescriptor makeCoreDescriptor(
      CoreContainer cc, String coreName, String isTransient, String loadOnStartup) {
    return new CoreDescriptor(
        coreName,
        cc.getCoreRootDirectory().resolve(coreName),
        cc,
        CoreDescriptor.CORE_TRANSIENT,
        isTransient,
        CoreDescriptor.CORE_LOADONSTARTUP,
        loadOnStartup);
  }

  private static final CoresLocator testCores =
      new ReadOnlyCoresLocator() {
        @Override
        public List<CoreDescriptor> discover(CoreContainer cc) {
          return List.of(
              makeCoreDescriptor(cc, "collection1", "false", "true"),
              makeCoreDescriptor(cc, "collection2", "true", "true"),
              makeCoreDescriptor(cc, "collection3", "on", "false"),
              makeCoreDescriptor(cc, "collection4", "false", "false"),
              makeCoreDescriptor(cc, "collection5", "false", "true"),
              makeCoreDescriptor(cc, "collection6", "true", "false"),
              makeCoreDescriptor(cc, "collection7", "true", "false"),
              makeCoreDescriptor(cc, "collection8", "true", "false"),
              makeCoreDescriptor(cc, "collection9", "true", "false"));
        }
      };

  private CoreContainer init() throws Exception {
    solrHomeDirectory = createTempDir();

    copyXmlToHome(solrHomeDirectory, "solr-transientCores.xml");
    for (int idx = 1; idx < 10; ++idx) {
      copyMinConf(solrHomeDirectory.resolve("collection" + idx));
    }

    NodeConfig cfg = NodeConfig.loadNodeConfig(solrHomeDirectory, null);
    return createCoreContainer(cfg, testCores);
  }

  private CoreContainer initEmpty() throws IOException {
    solrHomeDirectory = createTempDir();
    copyXmlToHome(solrHomeDirectory, "solr-transientCores.xml");
    NodeConfig cfg = NodeConfig.loadNodeConfig(solrHomeDirectory, null);
    return createCoreContainer(
        cfg,
        new ReadOnlyCoresLocator() {
          @Override
          public List<CoreDescriptor> discover(CoreContainer cc) {
            return Collections.emptyList();
          }
        });
  }

  @Test
  public void testLazyLoad() throws Exception {
    CoreContainer cc = init();
    try {

      // NOTE: This checks the initial state for loading, no need to do this elsewhere.
      checkLoadedCores(cc, "collection1", "collection2", "collection5");
      checkCoresNotLoaded(
          cc,
          "collection3",
          "collection4",
          "collection6",
          "collection7",
          "collection8",
          "collection9");

      SolrCore core1 = cc.getCore("collection1");
      assertFalse("core1 should not be transient", core1.getCoreDescriptor().isTransient());
      assertTrue("core1 should be loadable", core1.getCoreDescriptor().isLoadOnStartup());
      assertNotNull(core1.getSolrConfig());

      SolrCore core2 = cc.getCore("collection2");
      assertTrue("core2 should be transient", core2.getCoreDescriptor().isTransient());
      assertTrue("core2 should be loadable", core2.getCoreDescriptor().isLoadOnStartup());

      SolrCore core3 = cc.getCore("collection3");
      assertTrue("core3 should be transient", core3.getCoreDescriptor().isTransient());
      assertFalse("core3 should not be loadable", core3.getCoreDescriptor().isLoadOnStartup());

      SolrCore core4 = cc.getCore("collection4");
      assertFalse("core4 should not be transient", core4.getCoreDescriptor().isTransient());
      assertFalse("core4 should not be loadable", core4.getCoreDescriptor().isLoadOnStartup());

      SolrCore core5 = cc.getCore("collection5");
      assertFalse("core5 should not be transient", core5.getCoreDescriptor().isTransient());
      assertTrue("core5 should be loadable", core5.getCoreDescriptor().isLoadOnStartup());

      core1.close();
      core2.close();
      core3.close();
      core4.close();
      core5.close();
    } finally {
      cc.shutdown();
    }
  }

  // This is a little weak. I'm not sure how to test that lazy core2 is loaded automagically. The
  // getCore will, of course, load it.

  private void checkSearch(SolrCore core) throws IOException {
    addLazy(core, "id", "0");
    addLazy(core, "id", "1", "v_t", "Hello Dude");
    addLazy(core, "id", "2", "v_t", "Hello Yonik");
    addLazy(core, "id", "3", "v_s", "{!literal}");
    addLazy(core, "id", "4", "v_s", "other stuff");
    addLazy(core, "id", "5", "v_f", "3.14159");
    addLazy(core, "id", "6", "v_f", "8983");

    SolrQueryRequest req = makeReq(core);
    CommitUpdateCommand cmtCmd = new CommitUpdateCommand(req, false);
    core.getUpdateHandler().commit(cmtCmd);

    // Just get a couple of searches to work!
    assertQ(
        "test prefix query",
        makeReq(core, "q", "{!prefix f=v_t}hel", "wt", "xml"),
        "//result[@numFound='2']");

    assertQ(
        "test raw query",
        makeReq(core, "q", "{!raw f=v_t}hello", "wt", "xml"),
        "//result[@numFound='2']");

    // no analysis is done, so these should match nothing
    assertQ(
        "test raw query",
        makeReq(core, "q", "{!raw f=v_t}Hello", "wt", "xml"),
        "//result[@numFound='0']");
    assertQ(
        "test raw query",
        makeReq(core, "q", "{!raw f=v_f}1.5", "wt", "xml"),
        "//result[@numFound='0']");
  }

  @Test
  public void testLazySearch() throws Exception {
    CoreContainer cc = init();
    try {
      // Make sure Lazy4 isn't loaded. Should be loaded on the get
      checkCoresNotLoaded(cc, "collection4");
      SolrCore core4 = cc.getCore("collection4");

      checkSearch(core4);

      // Now just ensure that the normal searching on "collection1" finds _0_ on the same query that
      // found _2_ above. Use of makeReq above and req below is tricky, very tricky.
      SolrCore collection1 = cc.getCore("collection1");
      assertQ(
          "test raw query",
          makeReq(collection1, "q", "{!raw f=v_t}hello", "wt", "xml"),
          "//result[@numFound='0']");

      checkLoadedCores(cc, "collection1", "collection2", "collection4", "collection5");

      core4.close();
      collection1.close();
    } finally {
      cc.shutdown();
    }
  }

  @Test
  public void testCachingLimit() throws Exception {
    CoreContainer cc = init();
    try {
      // First check that all the cores that should be loaded at startup actually are.

      checkLoadedCores(cc, "collection1", "collection2", "collection5");
      checkCoresNotLoaded(
          cc,
          "collection3",
          "collection4",
          "collection6",
          "collection7",
          "collection8",
          "collection9");

      // By putting these in non-alpha order, we're also checking that we're  not just seeing an
      // artifact.
      getCoreAndPutBack(cc, "collection1");
      getCoreAndPutBack(cc, "collection3");
      getCoreAndPutBack(cc, "collection4");
      getCoreAndPutBack(cc, "collection2");
      getCoreAndPutBack(cc, "collection5");

      checkLoadedCores(
          cc, "collection1", "collection2", "collection3", "collection4", "collection5");
      checkCoresNotLoaded(cc, "collection6", "collection7", "collection8", "collection9");

      // map should be full up, add one more and verify
      getCoreAndPutBack(cc, "collection6");
      checkLoadedCores(
          cc,
          "collection1",
          "collection2",
          "collection3",
          "collection4",
          "collection5",
          "collection6");
      checkCoresNotLoaded(cc, "collection7", "collection8", "collection9");

      getCoreAndPutBack(cc, "collection7");
      checkLoadedCores(
          cc,
          "collection1",
          "collection2",
          "collection3",
          "collection4",
          "collection5",
          "collection6",
          "collection7");
      checkCoresNotLoaded(cc, "collection8", "collection9");

      getCoreAndPutBack(cc, "collection8");
      checkLoadedCores(cc, "collection1", "collection4", "collection5", "collection8");
      checkSomeLoadedCores(
          cc,
          TRANSIENT_CORE_CACHE_MAX_SIZE,
          "collection2",
          "collection3",
          "collection6",
          "collection7",
          "collection8");
      checkCoresNotLoaded(cc, "collection9");
      checkSomeCoresNotLoaded(
          cc,
          5 - TRANSIENT_CORE_CACHE_MAX_SIZE,
          "collection2",
          "collection3",
          "collection6",
          "collection7");

      getCoreAndPutBack(cc, "collection9");
      checkLoadedCores(cc, "collection1", "collection4", "collection5", "collection9");
      checkSomeLoadedCores(
          cc,
          TRANSIENT_CORE_CACHE_MAX_SIZE,
          "collection2",
          "collection3",
          "collection6",
          "collection7",
          "collection8",
          "collection9");
      checkSomeCoresNotLoaded(
          cc,
          6 - TRANSIENT_CORE_CACHE_MAX_SIZE,
          "collection2",
          "collection3",
          "collection6",
          "collection7",
          "collection8");

      // verify that getting metrics from an unloaded core doesn't cause exceptions (SOLR-12541)
      try (SolrCore core1 = cc.getCore("collection1");
          MetricsHandler handler = new MetricsHandler(h.getCoreContainer())) {

        SolrQueryResponse resp = new SolrQueryResponse();
        handler.handleRequest(makeReq(core1, CommonParams.QT, "/admin/metrics"), resp);
        NamedList<?> values = resp.getValues();
        assertNotNull(values.get("metrics"));
        values = (NamedList<?>) values.get("metrics");
        NamedList<?> nl = (NamedList<?>) values.get("solr.core.collection2");
        assertNotNull(nl);
        Object o = nl.get("REPLICATION./replication.indexPath");
        assertNotNull(o);
      }

    } finally {
      cc.shutdown();
    }
  }

  private void getCoreAndPutBack(CoreContainer cc, String name) {
    SolrCore core1 = cc.getCore(name);
    core1.close(); // note: the core can't have a refCount > 1 to be an eviction candidate
  }

  // Test case for SOLR-4300

  @Test
  public void testRace() throws Exception {
    final List<SolrCore> theCores = new ArrayList<>();
    final CoreContainer cc = init();
    try {

      Thread[] threads = new Thread[15];
      for (int idx = 0; idx < threads.length; idx++) {
        threads[idx] =
            new Thread() {
              @Override
              public void run() {
                SolrCore core = cc.getCore("collection3");
                synchronized (theCores) {
                  theCores.add(core);
                }
              }
            };
        threads[idx].start();
      }
      for (Thread thread : threads) {
        thread.join();
      }
      for (int idx = 0; idx < theCores.size() - 1; ++idx) {
        assertEquals("Cores should be the same!", theCores.get(idx), theCores.get(idx + 1));
      }
      for (SolrCore core : theCores) {
        core.close();
      }

    } finally {
      cc.shutdown();
    }
  }

  private void tryCreateFail(CoreAdminHandler admin, String name, String dataDir, String... errs) {
    SolrException thrown =
        expectThrows(
            SolrException.class,
            () -> {
              SolrQueryResponse resp = new SolrQueryResponse();

              SolrQueryRequest request =
                  req(
                      CoreAdminParams.ACTION,
                      CoreAdminParams.CoreAdminAction.CREATE.toString(),
                      CoreAdminParams.DATA_DIR,
                      dataDir,
                      CoreAdminParams.NAME,
                      name,
                      "schema",
                      "schema.xml",
                      "config",
                      "solrconfig.xml");

              admin.handleRequestBody(request, resp);
            });
    assertEquals("Exception code should be 500", 500, thrown.code());
    for (String err : errs) {
      assertTrue(
          "Should have seen an exception containing the an error",
          thrown.getMessage().contains(err));
    }
  }

  @Test
  public void testCreateSame() throws Exception {
    final CoreContainer cc = init();
    try {
      // First, try all 4 combinations of load on startup and transient
      final CoreAdminHandler admin = new CoreAdminHandler(cc);
      SolrCore lc2 = cc.getCore("collection2");
      SolrCore lc4 = cc.getCore("collection4");
      SolrCore lc5 = cc.getCore("collection5");
      SolrCore lc6 = cc.getCore("collection6");

      copyMinConf(solrHomeDirectory.resolve("t2"));
      copyMinConf(solrHomeDirectory.resolve("t4"));
      copyMinConf(solrHomeDirectory.resolve("t5"));
      copyMinConf(solrHomeDirectory.resolve("t6"));

      // Should also fail with the same name
      tryCreateFail(admin, "collection2", "t12", "Core with name", "collection2", "already exists");
      tryCreateFail(admin, "collection4", "t14", "Core with name", "collection4", "already exists");
      tryCreateFail(admin, "collection5", "t15", "Core with name", "collection5", "already exists");
      tryCreateFail(admin, "collection6", "t16", "Core with name", "collection6", "already exists");

      lc2.close();
      lc4.close();
      lc5.close();
      lc6.close();

    } finally {
      cc.shutdown();
    }
  }

  private void createViaAdmin(
      CoreContainer cc, String name, boolean isTransient, boolean loadOnStartup) throws Exception {

    try (final CoreAdminHandler admin = new CoreAdminHandler(cc)) {
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.CREATE.toString(),
              CoreAdminParams.NAME,
              name,
              CoreAdminParams.TRANSIENT,
              Boolean.toString(isTransient),
              CoreAdminParams.LOAD_ON_STARTUP,
              Boolean.toString(loadOnStartup)),
          resp);
    }
  }

  private void unloadViaAdmin(CoreContainer cc, String name) throws Exception {

    try (final CoreAdminHandler admin = new CoreAdminHandler(cc)) {
      SolrQueryResponse resp = new SolrQueryResponse();
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.UNLOAD.toString(),
              CoreAdminParams.CORE,
              name),
          resp);
    }
  }

  // Make sure that creating a transient core from the admin handler correctly respects the
  // transient limits etc.
  @Test
  public void testCreateTransientFromAdmin() throws Exception {
    final CoreContainer cc = initEmpty();
    try {
      copyMinConf(solrHomeDirectory.resolve("core1"));
      copyMinConf(solrHomeDirectory.resolve("core2"));
      copyMinConf(solrHomeDirectory.resolve("core3"));
      copyMinConf(solrHomeDirectory.resolve("core4"));
      copyMinConf(solrHomeDirectory.resolve("core5"));

      createViaAdmin(cc, "core1", true, true);
      createViaAdmin(cc, "core2", true, false);
      createViaAdmin(cc, "core3", true, true);
      createViaAdmin(cc, "core4", true, false);
      createViaAdmin(cc, "core5", true, false);

      final var coreNames = new String[] {"core1", "core2", "core3", "core4", "core5"};
      checkSomeCoresNotLoaded(cc, coreNames.length - TRANSIENT_CORE_CACHE_MAX_SIZE, coreNames);

      final SolrCore c1 = cc.getCore("core1");
      final SolrCore c2 = cc.getCore("core2");
      final SolrCore c3 = cc.getCore("core3");
      final SolrCore c4 = cc.getCore("core4");
      final SolrCore c5 = cc.getCore("core5");

      // no cores should be unloaded because we have references to them
      checkSomeCoresNotLoaded(cc, 0, coreNames);

      checkSomeLoadedCores(cc, 5, coreNames);

      // While we're at it, a test for SOLR-5366, unloading transient core that's been unloaded b/c
      // it's transient generates a "too many closes" error

      class TestThread extends Thread {

        @Override
        public void run() {

          final int sleep_millis = random().nextInt(1000);
          try {
            if (sleep_millis > 0) {
              if (VERBOSE) {
                System.out.println(
                    "TestLazyCores.testCreateTransientFromAdmin Thread.run sleeping for "
                        + sleep_millis
                        + " ms");
              }
              Thread.sleep(sleep_millis);
            }
          } catch (InterruptedException ie) {
            if (VERBOSE) {
              System.out.println(
                  "TestLazyCores.testCreateTransientFromAdmin Thread.run caught "
                      + ie
                      + " whilst sleeping for "
                      + sleep_millis
                      + " ms");
            }
          }

          c1.close();
          c2.close();
          c3.close();
          c4.close();
          c5.close();
        }
      }
      ;

      // with SOLR-6279 UNLOAD will wait for the core's reference count to have reached zero
      // hence cN.close() need to proceed or run in parallel with unloadViaAdmin(...)
      final TestThread cThread = new TestThread();
      cThread.start();

      unloadViaAdmin(cc, "core1");
      unloadViaAdmin(cc, "core2");
      unloadViaAdmin(cc, "core3");
      unloadViaAdmin(cc, "core4");
      unloadViaAdmin(cc, "core5");

      cThread.join();

    } finally {
      cc.shutdown();
    }
  }

  // Test that transient cores
  // 1> produce errors as appropriate when the config or schema files are foo'd
  // 2> "self-heal". That is, if the problem is corrected can the core be reloaded and used?
  // 3> that OK cores can be searched even when some cores failed to load.
  // 4> that having no solr.xml entry for transient cache handler correctly uses the default.
  @Test
  public void testBadConfigsGenerateErrors() throws Exception {
    final CoreContainer cc =
        initGoodAndBad(
            Arrays.asList("core1", "core2"),
            Arrays.asList("badSchema1", "badSchema2"),
            Arrays.asList("badConfig1", "badConfig2"));

    try {
      // first, did the two good cores load successfully?
      checkLoadedCores(cc, "core1", "core2");

      // Did the bad cores fail to load?
      checkFailedCores(cc, "badSchema1", "badSchema2", "badConfig1", "badConfig2");

      //  Can we still search the "good" cores even though there were core init failures?
      SolrCore core1 = cc.getCore("core1");
      checkSearch(core1);

      // Did we get the expected message for each of the cores that failed to load? Make sure we
      // don't run afoul of the dreaded slash/backslash difference on Windows and *nix machines.
      testMessage(
          cc.getCoreInitFailures(), Path.of("badConfig1", "conf", "solrconfig.xml").toString());
      testMessage(
          cc.getCoreInitFailures(), Path.of("badConfig2", "conf", "solrconfig.xml").toString());
      testMessage(cc.getCoreInitFailures(), Path.of("badSchema1", "conf", "schema.xml").toString());
      testMessage(cc.getCoreInitFailures(), Path.of("badSchema2", "conf", "schema.xml").toString());

      // Status should report that there are failure messages for the bad cores and none for the
      // good cores.
      checkStatus(cc, true, "core1");
      checkStatus(cc, true, "core2");
      checkStatus(cc, false, "badSchema1");
      checkStatus(cc, false, "badSchema2");
      checkStatus(cc, false, "badConfig1");
      checkStatus(cc, false, "badConfig2");

      // Copy good config and schema files in and see if you can then load them (they are transient
      // after all)
      copyGoodConf("badConfig1", "solrconfig-minimal.xml", "solrconfig.xml");
      copyGoodConf("badConfig2", "solrconfig-minimal.xml", "solrconfig.xml");
      copyGoodConf("badSchema1", "schema-tiny.xml", "schema.xml");
      copyGoodConf("badSchema2", "schema-tiny.xml", "schema.xml");

      // Reload the cores and ensure that
      // 1> they pick up the new configs
      // 2> they don't fail again b/c they still have entries in loadFailure in core container.
      cc.reload("badConfig1");
      cc.reload("badConfig2");
      cc.reload("badSchema1");
      cc.reload("badSchema2");
      SolrCore bc1 = cc.getCore("badConfig1");
      ;
      SolrCore bc2 = cc.getCore("badConfig2");
      SolrCore bs1 = cc.getCore("badSchema1");
      SolrCore bs2 = cc.getCore("badSchema2");

      // all the cores should be found in the list now.
      checkLoadedCores(
          cc, "core1", "core2", "badSchema1", "badSchema2", "badConfig1", "badConfig2");

      // Did we clear out the errors by putting good files in place? And the cores that never were
      // bad should be OK too.
      checkStatus(cc, true, "core1");
      checkStatus(cc, true, "core2");
      checkStatus(cc, true, "badSchema1");
      checkStatus(cc, true, "badSchema2");
      checkStatus(cc, true, "badConfig1");
      checkStatus(cc, true, "badConfig2");

      // Are the formerly bad cores now searchable? Testing one of each should do.
      checkSearch(core1);
      checkSearch(bc1);
      checkSearch(bs1);

      core1.close();
      bc1.close();
      bc2.close();
      bs1.close();
      bs2.close();
    } finally {
      cc.shutdown();
    }
  }

  // See fi the message you expect is in the list of failures
  private void testMessage(Map<String, CoreContainer.CoreLoadFailure> failures, String lookFor) {
    List<String> messages = new ArrayList<>();
    for (CoreContainer.CoreLoadFailure e : failures.values()) {
      String message = e.exception.getCause().getMessage();
      messages.add(message);
      if (message.contains(lookFor)) return;
    }
    fail(
        "Should have found message containing these tokens "
            + lookFor
            + " in the failure messages: "
            + messages);
  }

  // Just localizes writing a configuration rather than repeating it for good and bad files.
  private void writeCustomConfig(String coreName, String config, String schema, String rand_snip)
      throws IOException {

    Path coreRoot = solrHomeDirectory.resolve(coreName);
    Path subHome = coreRoot.resolve("conf");
    Files.createDirectories(subHome);
    // Write the file for core discovery
    Files.writeString(
        coreRoot.resolve("core.properties"),
        "name="
            + coreName
            + System.getProperty("line.separator")
            + "transient=true"
            + System.getProperty("line.separator")
            + "loadOnStartup=true",
        StandardCharsets.UTF_8);

    Files.writeString(
        subHome.resolve("solrconfig.snippet.randomindexconfig.xml"),
        rand_snip,
        StandardCharsets.UTF_8);

    Files.writeString(subHome.resolve("solrconfig.xml"), config, StandardCharsets.UTF_8);

    Files.writeString(subHome.resolve("schema.xml"), schema, StandardCharsets.UTF_8);
  }

  // Write out the cores' config files, both bad schema files, bad config files and some good
  // cores.
  private CoreContainer initGoodAndBad(
      List<String> goodCores, List<String> badSchemaCores, List<String> badConfigCores)
      throws Exception {
    solrHomeDirectory = createTempDir();

    // Don't pollute the log with exception traces when they're expected.
    ignoreException(Pattern.quote("SAXParseException"));

    // Create the cores that should be fine.
    for (String coreName : goodCores) {
      Path coreRoot = solrHomeDirectory.resolve(coreName);
      copyMinConf(coreRoot, "name=" + coreName);
    }

    // Collect the files that we'll write to the config directories.
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    String min_schema = Files.readString(Path.of(top, "schema-tiny.xml"), StandardCharsets.UTF_8);
    String min_config =
        Files.readString(Path.of(top, "solrconfig-minimal.xml"), StandardCharsets.UTF_8);
    String rand_snip =
        Files.readString(
            Path.of(top, "solrconfig.snippet.randomindexconfig.xml"), StandardCharsets.UTF_8);

    // Now purposely mess up the config files, introducing stupid syntax errors.
    String bad_config = min_config.replace("<requestHandler", "<reqsthalr");
    String bad_schema = min_schema.replace("<field", "<filed");

    // Create the cores with bad configs
    for (String coreName : badConfigCores) {
      writeCustomConfig(coreName, bad_config, min_schema, rand_snip);
    }

    // Create the cores with bad schemas.
    for (String coreName : badSchemaCores) {
      writeCustomConfig(coreName, min_config, bad_schema, rand_snip);
    }

    NodeConfig config = SolrXmlConfig.fromString(solrHomeDirectory, "<solr/>");

    // OK this should succeed, but at the end we should have recorded a series of errors.
    return createCoreContainer(config, new CorePropertiesLocator(config));
  }

  // We want to see that the core "heals itself" if an un-corrupted file is written to the
  // directory.
  private void copyGoodConf(String coreName, String srcName, String dstName) throws IOException {
    Path coreRoot = solrHomeDirectory.resolve(coreName);
    Path subHome = coreRoot.resolve("conf");
    String top = SolrTestCaseJ4.TEST_HOME() + "/collection1/conf";
    Files.copy(
        Path.of(top, srcName), subHome.resolve(dstName), StandardCopyOption.REPLACE_EXISTING);
  }

  // If ok==true, we shouldn't be seeing any failure cases.
  // if ok==false, the core being examined should have a failure in the list.
  private void checkStatus(CoreContainer cc, Boolean ok, String core) throws Exception {
    SolrQueryResponse resp = new SolrQueryResponse();
    try (final CoreAdminHandler admin = new CoreAdminHandler(cc)) {
      admin.handleRequestBody(
          req(
              CoreAdminParams.ACTION,
              CoreAdminParams.CoreAdminAction.STATUS.toString(),
              CoreAdminParams.CORE,
              core),
          resp);
    }

    @SuppressWarnings({"unchecked"})
    Map<String, Exception> failures = (Map<String, Exception>) resp.getValues().get("initFailures");

    if (ok) {
      if (failures.size() != 0) {
        fail("Should have cleared the error, but there are failures " + failures);
      }
    } else {
      if (failures.size() == 0) {
        fail("Should have had errors here but the status return has no failures!");
      }
    }
  }

  public static void checkCoresNotLoaded(CoreContainer cc, String... coreNames) {
    checkSomeCoresNotLoaded(cc, coreNames.length, coreNames);
  }

  public static void checkSomeCoresNotLoaded(
      CoreContainer cc, int numNotLoaded, String... coreNames) {
    Collection<String> loadedCoreNames = cc.getLoadedCoreNames();
    List<String> notLoadedCoreNames = new ArrayList<>();
    for (String coreName : coreNames) {
      if (!loadedCoreNames.contains(coreName)) {
        notLoadedCoreNames.add(coreName);
      }
    }
    assertEquals(
        "Expected "
            + numNotLoaded
            + " not loaded cores but found "
            + notLoadedCoreNames.size()
            + ", coreNames="
            + Arrays.asList(coreNames)
            + ", notLoadedCoreNames="
            + notLoadedCoreNames
            + ", loadedCoreNames="
            + loadedCoreNames,
        numNotLoaded,
        notLoadedCoreNames.size());

    // All transient cores are listed in allCoreNames.
    Collection<String> allCoreNames = cc.getAllCoreNames();
    for (String coreName : coreNames) {
      assertTrue(
          "Core " + coreName + " should have been found in the list of all known core names",
          allCoreNames.contains(coreName));
    }

    checkCoreNamesAndDescriptors(cc);
  }

  private static void checkCoreNamesAndDescriptors(CoreContainer cc) {
    Collection<String> allNames = cc.getAllCoreNames();
    List<CoreDescriptor> descriptors = cc.getCoreDescriptors();

    // Every core that has not failed to load should be in coreDescriptors.
    assertEquals(
        "There should be as many coreDescriptors as coreNames",
        allNames.size(),
        descriptors.size());
    for (CoreDescriptor desc : descriptors) {
      assertTrue(
          "Each coreName should have a corresponding coreDescriptor",
          allNames.contains(desc.getName()));
    }

    // All loaded cores are in allNames.
    for (String name : cc.getLoadedCoreNames()) {
      assertTrue(
          "Loaded core " + name + " should have been found in the list of all possible core names",
          allNames.contains(name));
    }
  }

  private static void checkFailedCores(CoreContainer cc, String... failedCoreNames) {
    // Failed cores should not be in allCoreNames.
    Collection<String> allNames = cc.getAllCoreNames();
    for (String name : failedCoreNames) {
      assertFalse(
          "Failed core "
              + name
              + " should not have been found in the list of all possible core names",
          allNames.contains(name));
    }
  }

  public static void checkLoadedCores(CoreContainer cc, String... coreNames) {
    checkSomeLoadedCores(cc, coreNames.length, coreNames);
  }

  public static void checkSomeLoadedCores(CoreContainer cc, int numLoaded, String... coreNames) {
    Collection<String> loadedCoreNames = cc.getLoadedCoreNames();
    List<String> loadedListedCoreNames = new ArrayList<>();
    for (String coreName : coreNames) {
      if (loadedCoreNames.contains(coreName)) {
        loadedListedCoreNames.add(coreName);
      }
    }
    assertEquals(
        "Expected "
            + numLoaded
            + " loaded cores but found "
            + loadedListedCoreNames.size()
            + ", coreNames="
            + Arrays.asList(coreNames)
            + ", loadedListedCoreNames="
            + loadedListedCoreNames
            + ", loadedCoreNames="
            + loadedCoreNames,
        numLoaded,
        loadedListedCoreNames.size());
  }

  private void addLazy(SolrCore core, String... fieldValues) throws IOException {
    UpdateHandler updater = core.getUpdateHandler();
    AddUpdateCommand cmd = new AddUpdateCommand(makeReq(core));
    cmd.solrDoc = sdoc((Object[]) fieldValues);
    updater.addDoc(cmd);
  }

  private LocalSolrQueryRequest makeReq(SolrCore core, String... paramPairs) {
    return new LocalSolrQueryRequest(core, params(paramPairs));
  }

  @Test
  public void testMidUseUnload() throws Exception {
    // sleep for up to 10 s Must add 1 because using
    final int maximumSleepMillis = random().nextInt(9999) + 1;
    // this as a seed will rea few lines down will
    // throw an exception if this is zero
    if (VERBOSE) {
      System.out.println("TestLazyCores.testMidUseUnload maximumSleepMillis=" + maximumSleepMillis);
    }

    class TestThread extends Thread {

      SolrCore core_to_use = null;

      @Override
      public void run() {

        final int sleep_millis = random().nextInt(maximumSleepMillis);
        try {
          if (sleep_millis > 0) {
            if (VERBOSE) {
              System.out.println(
                  "TestLazyCores.testMidUseUnload Thread.run sleeping for " + sleep_millis + " ms");
            }
            Thread.sleep(sleep_millis);
          }
        } catch (InterruptedException ie) {
          if (VERBOSE) {
            System.out.println(
                "TestLazyCores.testMidUseUnload Thread.run caught "
                    + ie
                    + " whilst sleeping for "
                    + sleep_millis
                    + " ms");
          }
        }

        // not closed since we are still using it and hold a reference
        assertFalse(core_to_use.isClosed());
        // now give up our reference to the core
        core_to_use.close();
      }
    }

    CoreContainer cc = init();

    try {
      TestThread thread = new TestThread();

      thread.core_to_use = cc.getCore("collection1");
      assertNotNull(thread.core_to_use);
      assertFalse(thread.core_to_use.isClosed()); // freshly-in-use core is not closed
      thread.start();

      unloadViaAdmin(cc, "collection1");
      assertTrue(thread.core_to_use.isClosed()); // after unload-ing the core is closed

      thread.join();
    } finally {
      cc.shutdown();
    }
  }

  // Ensure that when a core is evicted from the transient cache, any uncommitted docs are
  // preserved. Note, this needs FS-based indexes to persist! Cores 2, 3, 6, 7, 8, 9 are transient
  @Test
  public void testNoCommit() throws Exception {
    CoreContainer cc = init();
    String[] transientCoreNames =
        new String[] {
          "collection2", "collection3", "collection6", "collection7", "collection8", "collection9"
        };
    try {
      // First, go through all the transient cores and add some docs. DO NOT COMMIT!
      // The evicted core should commit the docs when it gets closed.
      for (String coreName : transientCoreNames) {
        try (SolrCore core = cc.getCore(coreName)) {
          add10(core);
        }
      }

      // Just proving that some cores have been evicted to respect transient core cache max size.
      checkSomeCoresNotLoaded(
          cc, transientCoreNames.length - TRANSIENT_CORE_CACHE_MAX_SIZE, transientCoreNames);

      // We still should have 4 transient cores loaded, their reference counts have NOT dropped to
      // zero
      checkLoadedCores(cc, "collection1", "collection5");
      checkSomeLoadedCores(cc, TRANSIENT_CORE_CACHE_MAX_SIZE, transientCoreNames);

      Collection<String> loadedCoreNames = cc.getLoadedCoreNames();
      int notLoadedCoreCount = 0;
      List<SolrCore> openCores = new ArrayList<>();
      for (String coreName : transientCoreNames) {
        // The point of this test is to ensure that when cores are evicted and re-opened
        // that the docs are there, so ensure that the core we're testing is gone, gone, gone.
        if (!loadedCoreNames.contains(coreName)) {
          notLoadedCoreCount++;
          checkCoresNotLoaded(cc, coreName);

          // Load the core up again.
          SolrCore core = cc.getCore(coreName);
          openCores.add(core);
          checkLoadedCores(cc, coreName);

          // Insure docs are still there.
          check10(core);
        }
      }
      assertEquals(transientCoreNames.length - TRANSIENT_CORE_CACHE_MAX_SIZE, notLoadedCoreCount);
      openCores.forEach(SolrCore::close);
    } finally {
      cc.shutdown();
    }
  }

  private void add10(SolrCore core) throws IOException {
    for (int idx = 0; idx < 10; ++idx) {
      addLazy(core, "id", "0" + idx);
    }
    SolrQueryRequest req = makeReq(core);
  }

  private void check10(SolrCore core) {
    // Just get a couple of searches to work!
    assertQ(
        "test closing core without committing",
        makeReq(core, "q", "*:*"),
        "//result[@numFound='10']");
  }

  public void testDontEvictUsedCore() throws Exception {
    // If a core is being used for a long time (say a long indexing batch) but nothing else for it,
    // and if the transient cache has pressure and thus wants to unload a core, we should not
    // unload it (yet).

    CoreContainer cc = init();
    String[] transientCoreNames =
        new String[] {
          "collection2", "collection3", "collection6", "collection7", "collection8", "collection9"
        };

    try (LogListener logs =
        LogListener.info(TransientSolrCoreCacheDefault.class.getName())
            .substring("NOT evicting transient core [" + transientCoreNames[0] + "]")) {
      cc.waitForLoadingCoresToFinish(1000);
      var solr = new EmbeddedSolrServer(cc, null);
      final var longReqTimeMs = 5000; // plenty of time for a slow/busy CI

      // First, start a long request on the first transient core.
      //  We do this via relying on EmbeddedSolrServer to keep the core open as it works with
      //  this streaming callback mechanism.
      var longRequestLatch = new CountDownLatch(1);
      var thread =
          new Thread("longRequest") {
            @Override
            public void run() {
              try {
                solr.queryAndStreamResponse(
                    transientCoreNames[0],
                    params("q", "*:*"),
                    new StreamingResponseCallback() {
                      @Override
                      public void streamSolrDocument(SolrDocument doc) {}

                      @Override
                      public void streamDocListInfo(long numFound, long start, Float maxScore) {
                        try {
                          // the core remains open until the test calls countDown()
                          longRequestLatch.await();
                        } catch (InterruptedException e) {
                          Thread.currentThread().interrupt();
                          throw new RuntimeException(e);
                        }
                      }
                    });
              } catch (SolrServerException | IOException e) {
                fail(e.toString());
              }
            }
          };
      thread.start();

      System.out.println("Inducing pressure on cache by querying many cores...");
      // Now hammer on other transient cores to create transient cache pressure
      for (int round = 0; round < 5 && logs.getCount() == 0; round++) {
        // note: we skip over the first; we want the first to remain non-busy
        for (int i = 1; i < transientCoreNames.length; i++) {
          solr.query(transientCoreNames[i], params("q", "*:*"));
        }
      }
      // Show that the cache logs that it was asked to evict but did not.
      // To show the bug behavior, comment this out and also comment out the corresponding logic
      // that fixes it at the spot this message is logged.
      assertTrue(logs.getCount() > 0);

      System.out.println("Done inducing pressure; now load first core");
      assertTrue("long request should still be busy", thread.isAlive());
      // Do another request on the first core
      solr.query(transientCoreNames[0], params("q", "id:wakeUp"));

      longRequestLatch.countDown();
      thread.join(longReqTimeMs);
      assertFalse(thread.isAlive());

      // Do another request on the first core
      solr.query(transientCoreNames[0], params("q", "id:justCheckingAgain"));

      logs.getQueue().clear();
    } finally {
      cc.shutdown();
    }
  }
}
