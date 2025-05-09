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
package org.apache.solr.client.solrj.request;

import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.core.Is.is;

import com.codahale.metrics.MetricRegistry;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import org.apache.commons.io.file.PathUtils;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.AbstractEmbeddedSolrServerTestCase;
import org.apache.solr.client.solrj.request.CoreAdminRequest.Create;
import org.apache.solr.client.solrj.request.CoreAdminRequest.RequestRecovery;
import org.apache.solr.client.solrj.response.CoreAdminResponse;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.metrics.SolrCoreMetricManager;
import org.apache.solr.metrics.SolrMetricManager;
import org.junit.Test;

public class TestCoreAdmin extends AbstractEmbeddedSolrServerTestCase {

  @Test
  public void testConfigSet() throws Exception {

    SolrClient client = getSolrAdmin();
    Path testDir = createTempDir(LuceneTestCase.getTestClass().getSimpleName());
    Path newCoreInstanceDir = testDir.resolve("newcore");
    cores.getAllowPaths().add(testDir); // Allow the test dir

    CoreAdminRequest.Create req = new CoreAdminRequest.Create();
    req.setCoreName("corewithconfigset");
    req.setInstanceDir(newCoreInstanceDir.toString());
    req.setConfigSet("configset-2");

    CoreAdminResponse response = req.process(client);
    assertThat((String) response.getResponse().get("core"), is("corewithconfigset"));

    try (SolrCore core = cores.getCore("corewithconfigset")) {
      assertThat(core, is(notNullValue()));
    }
  }

  @Test
  public void testCustomUlogDir() throws Exception {

    try (SolrClient client = getSolrAdmin()) {

      Path dataDir = createTempDir("data");

      Path newCoreInstanceDir = createTempDir("instance");
      cores.getAllowPaths().add(dataDir); // Allow the test dir
      cores.getAllowPaths().add(newCoreInstanceDir); // Allow the test dir

      Path instanceDir = cores.getSolrHome();
      PathUtils.copyDirectory(instanceDir, newCoreInstanceDir.resolve("newcore"));

      CoreAdminRequest.Create req = new CoreAdminRequest.Create();
      req.setCoreName("newcore");
      req.setInstanceDir(newCoreInstanceDir.resolve("newcore").toString());
      req.setDataDir(dataDir.toString());
      req.setUlogDir(dataDir.resolve("ulog").toString());
      req.setConfigSet("shared");

      // These should be the inverse of defaults.
      req.setIsLoadOnStartup(false);
      req.setIsTransient(true);
      req.process(client);

      // Show that the newly-created core has values for load on startup and transient that differ
      // from defaults due to the above.
      Path logDir;
      try (SolrCore coreProveIt = cores.getCore("collection1");
          SolrCore core = cores.getCore("newcore")) {

        assertTrue(core.getCoreDescriptor().isTransient());
        assertFalse(coreProveIt.getCoreDescriptor().isTransient());

        assertFalse(core.getCoreDescriptor().isLoadOnStartup());
        assertTrue(coreProveIt.getCoreDescriptor().isLoadOnStartup());

        logDir = Path.of(core.getUpdateHandler().getUpdateLog().getTlogDir());
      }

      assertEquals(dataDir.resolve("ulog").resolve("tlog").toString(), logDir.toString());
    }
  }

  @Test
  public void testErrorCases() throws Exception {

    ModifiableSolrParams params = new ModifiableSolrParams();
    params.set("action", "BADACTION");
    String collectionName = "badactioncollection";
    params.set("name", collectionName);
    QueryRequest request = new QueryRequest(params);
    request.setPath("/admin/cores");
    expectThrows(SolrException.class, () -> getSolrAdmin().request(request));
  }

  @Test
  public void testInvalidCoreNamesAreRejectedWhenCreatingCore() {
    final Create createRequest = new Create();
    SolrException e =
        expectThrows(SolrException.class, () -> createRequest.setCoreName("invalid$core@name"));
    final String exceptionMessage = e.getMessage();
    assertTrue(exceptionMessage.contains("Invalid core"));
    assertTrue(exceptionMessage.contains("invalid$core@name"));
    assertTrue(
        exceptionMessage.contains(
            "must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }

  @Test
  public void testInvalidCoreNamesAreRejectedWhenRenamingExistingCore() throws Exception {
    SolrException e =
        expectThrows(
            SolrException.class,
            () -> CoreAdminRequest.renameCore("validExistingCoreName", "invalid$core@name", null));
    final String exceptionMessage = e.getMessage();
    assertTrue(e.getMessage(), exceptionMessage.contains("Invalid core"));
    assertTrue(exceptionMessage.contains("invalid$core@name"));
    assertTrue(
        exceptionMessage.contains(
            "must consist entirely of periods, underscores, hyphens, and alphanumerics"));
  }

  @Test
  public void testValidCoreRename() throws Exception {
    Collection<String> names = cores.getAllCoreNames();
    assertFalse(names.toString(), names.contains("coreRenamed"));
    assertTrue(names.toString(), names.contains("core1"));
    CoreAdminRequest.renameCore("core1", "coreRenamed", getSolrAdmin());
    names = cores.getAllCoreNames();
    assertTrue(names.toString(), names.contains("coreRenamed"));
    assertFalse(names.toString(), names.contains("core1"));
    // rename it back
    CoreAdminRequest.renameCore("coreRenamed", "core1", getSolrAdmin());
    names = cores.getAllCoreNames();
    assertFalse(names.toString(), names.contains("coreRenamed"));
    assertTrue(names.toString(), names.contains("core1"));
    assertEquals(names.size(), cores.getNumAllCores());
  }

  @Test
  public void testCoreSwap() throws Exception {
    // index marker docs to core0
    SolrClient cli0 = getSolrCore0();
    SolrInputDocument d = new SolrInputDocument("id", "core0-0");
    cli0.add(d);
    d = new SolrInputDocument("id", "core0-1");
    cli0.add(d);
    cli0.commit();
    // index a marker doc to core1
    SolrClient cli1 = getSolrCore1();
    d = new SolrInputDocument("id", "core1-0");
    cli1.add(d);
    cli1.commit();

    // initial state assertions
    SolrQuery q = new SolrQuery("*:*");
    QueryResponse rsp = cli0.query(q);
    SolrDocumentList docs = rsp.getResults();
    assertEquals(2, docs.size());
    docs.forEach(
        doc -> {
          assertTrue(doc.toString(), doc.getFieldValue("id").toString().startsWith("core0-"));
        });

    rsp = cli1.query(q);
    docs = rsp.getResults();
    assertEquals(1, docs.size());
    docs.forEach(
        doc -> {
          assertTrue(doc.toString(), doc.getFieldValue("id").toString().startsWith("core1-"));
        });

    // assert initial metrics
    SolrMetricManager metricManager = cores.getMetricManager();
    String core0RegistryName =
        SolrCoreMetricManager.createRegistryName(false, null, null, null, "core0");
    String core1RegistryName =
        SolrCoreMetricManager.createRegistryName(false, null, null, null, "core1");
    MetricRegistry core0Registry = metricManager.registry(core0RegistryName);
    MetricRegistry core1Registry = metricManager.registry(core1RegistryName);

    // 2 docs + 1 commit
    assertEquals(3, core0Registry.counter("UPDATE./update.requests").getCount());
    // 1 doc + 1 commit
    assertEquals(2, core1Registry.counter("UPDATE./update.requests").getCount());

    // swap
    CoreAdminRequest.swapCore("core0", "core1", getSolrAdmin());

    // assert state after swap
    cli0 = getSolrCore0();
    cli1 = getSolrCore1();

    rsp = cli0.query(q);
    docs = rsp.getResults();
    assertEquals(1, docs.size());
    docs.forEach(
        doc -> {
          assertTrue(doc.toString(), doc.getFieldValue("id").toString().startsWith("core1-"));
        });

    rsp = cli1.query(q);
    docs = rsp.getResults();
    assertEquals(2, docs.size());
    docs.forEach(
        doc -> {
          assertTrue(doc.toString(), doc.getFieldValue("id").toString().startsWith("core0-"));
        });

    core0Registry = metricManager.registry(core0RegistryName);
    core1Registry = metricManager.registry(core1RegistryName);

    assertEquals(2, core0Registry.counter("UPDATE./update.requests").getCount());
    assertEquals(3, core1Registry.counter("UPDATE./update.requests").getCount());
  }

  @Test
  public void testInvalidRequestRecovery() throws SolrServerException, IOException {
    RequestRecovery recoverRequestCmd = new RequestRecovery();
    recoverRequestCmd.setCoreName("non_existing_core");
    expectThrows(SolrException.class, () -> recoverRequestCmd.process(getSolrAdmin()));
  }

  @Test
  public void testReloadCoreAfterFailure() throws Exception {
    cores.shutdown();
    useFactory(null); // use FS factory

    try {
      cores = CoreContainer.createAndLoad(SOLR_HOME);

      String ddir = CoreAdminRequest.getCoreStatus("core0", getSolrCore0()).dataDir;
      Path data = Path.of(ddir, "index");
      assumeTrue("test can't handle relative data directory paths (yet?)", data.isAbsolute());

      getSolrCore0().add(new SolrInputDocument("id", "core0-1"));
      getSolrCore0().commit();

      cores.shutdown();

      // destroy the index
      Files.move(data.resolve("_0.si"), data.resolve("backup"));
      cores = CoreContainer.createAndLoad(SOLR_HOME);

      // Need to run a query to confirm that the core couldn't load
      expectThrows(SolrException.class, () -> getSolrCore0().query(new SolrQuery("*:*")));

      // We didn't fix anything, so should still throw
      expectThrows(SolrException.class, () -> CoreAdminRequest.reloadCore("core0", getSolrCore0()));

      Files.move(data.resolve("backup"), data.resolve("_0.si"));
      CoreAdminRequest.reloadCore("core0", getSolrCore0());
      assertEquals(1, getSolrCore0().query(new SolrQuery("*:*")).getResults().getNumFound());
    } finally {
      cores.shutdown();
      resetFactory();
    }
  }
}
