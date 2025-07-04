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
package org.apache.solr.ltr.feature;

import java.io.IOException;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.ltr.FeatureLoggerTestUtils;
import org.apache.solr.ltr.TestRerankBase;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.DefaultValueFieldValueFeatureScorer;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.FieldValueFeatureScorer;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.NumericDocValuesFieldValueFeatureScorer;
import org.apache.solr.ltr.feature.FieldValueFeature.FieldValueFeatureWeight.SortedDocValuesFieldValueFeatureScorer;
import org.apache.solr.ltr.model.LinearModel;
import org.apache.solr.request.SolrQueryRequest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestFieldValueFeature extends TestRerankBase {

  private static final float FIELD_VALUE_FEATURE_DEFAULT_VAL = 0.0f;

  private static final String[] FIELDS = {
    "popularity",
    "dvIntPopularity",
    "dvLongPopularity",
    "dvFloatPopularity",
    "dvDoublePopularity",
    "dvStringPopularity",
    "isTrendy",
    "dvIsTrendy",
    "storedDvIsTrendy"
  };

  protected String getFieldValueFeatureClassName() {
    return FieldValueFeature.class.getName();
  }

  protected String getObservingFieldValueFeatureClassName() {
    return ObservingFieldValueFeature.class.getName();
  }

  @Before
  public void before() throws Exception {
    setuptest(false);

    assertU(
        adoc(
            "id",
            "1",
            "popularity",
            "1",
            "title",
            "w1",
            "dvStringPopularity",
            "1",
            "description",
            "w1",
            "isTrendy",
            "true"));
    assertU(
        adoc(
            "id",
            "2",
            "popularity",
            "2",
            "title",
            "w2 2asd asdd didid",
            "dvStringPopularity",
            "2",
            "description",
            "w2 2asd asdd didid"));
    assertU(
        adoc(
            "id",
            "3",
            "popularity",
            "3",
            "title",
            "w3",
            "dvStringPopularity",
            "3",
            "description",
            "w3",
            "isTrendy",
            "true"));
    assertU(
        adoc(
            "id",
            "4",
            "popularity",
            "4",
            "title",
            "w4",
            "dvStringPopularity",
            "4",
            "description",
            "w4",
            "isTrendy",
            "false"));
    assertU(
        adoc(
            "id",
            "5",
            "popularity",
            "5",
            "title",
            "w5",
            "dvStringPopularity",
            "5",
            "description",
            "w5",
            "isTrendy",
            "true"));
    assertU(
        adoc(
            "id",
            "6",
            "popularity",
            "6",
            "title",
            "w1 w2",
            "dvStringPopularity",
            "6",
            "description",
            "w1 w2",
            "isTrendy",
            "false"));
    assertU(
        adoc(
            "id",
            "7",
            "popularity",
            "7",
            "title",
            "w1 w2 w3 w4 w5",
            "dvStringPopularity",
            "7",
            "description",
            "w1 w2 w3 w4 w5 w8",
            "isTrendy",
            "true"));
    assertU(
        adoc(
            "id",
            "8",
            "popularity",
            "8",
            "title",
            "w1 w1 w1 w2 w2 w8",
            "dvStringPopularity",
            "8",
            "description",
            "w1 w1 w1 w2 w2",
            "isTrendy",
            "false"));

    // a document without the popularity and the dv fields
    assertU(
        adoc(
            "id",
            "42",
            "title",
            "NO popularity or isTrendy",
            "description",
            "NO popularity or isTrendy"));

    assertU(commit());

    for (String field : FIELDS) {
      loadFeature(field, getFieldValueFeatureClassName(), "{\"field\":\"" + field + "\"}");
    }
    loadModel(
        "model",
        LinearModel.class.getName(),
        FIELDS,
        "{\"weights\":{\"popularity\":1.0,\"dvIntPopularity\":1.0,\"dvLongPopularity\":1.0,"
            + "\"dvFloatPopularity\":1.0,\"dvDoublePopularity\":1.0,"
            + "\"dvStringPopularity\":1.0,\"isTrendy\":1.0,\"dvIsTrendy\":1.0,\"storedDvIsTrendy\":1.0}}");
  }

  @After
  public void after() throws Exception {
    aftertest();
  }

  @Test
  public void testRanking() throws Exception {

    final SolrQuery query = new SolrQuery();
    query.setQuery("title:w1");
    query.add("fl", "*, score");
    query.add("rows", "4");

    // Normal term match
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='1'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='7'");

    query.add("rq", "{!ltr model=model reRankDocs=4}");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==4");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='1'");

    query.setQuery("*:*");
    query.remove("rows");
    query.add("rows", "8");
    query.remove("rq");
    query.add("rq", "{!ltr model=model reRankDocs=8}");

    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='8'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[1]/id=='7'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[2]/id=='6'");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[3]/id=='5'");
  }

  @Test
  public void testIfADocumentDoesntHaveAFieldDefaultValueIsReturned() throws Exception {
    SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("fl", "*, score");
    query.add("rows", "4");

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='42'");

    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "popularity",
            "0.0",
            "dvIntPopularity",
            "0.0",
            "dvLongPopularity",
            "0.0",
            "dvFloatPopularity",
            "0.0",
            "dvDoublePopularity",
            "0.0",
            "dvStringPopularity",
            "0.0",
            "isTrendy",
            "0.0",
            "dvIsTrendy",
            "0.0",
            "storedDvIsTrendy",
            "0.0");
    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=model reRankDocs=4}");
    query.add("fl", "[fv]");

    // "0.0" in the assertJQ below is more readable than
    // Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL) but first make sure it's equivalent
    assertEquals("0.0", Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL));

    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
  }

  @Test
  public void testIfADocumentDoesntHaveAFieldASetDefaultValueIsReturned() throws Exception {
    for (String field : FIELDS) {
      final String fstore = "testIfADocumentDoesntHaveAFieldASetDefaultValueIsReturned" + field;

      loadFeature(
          field + "42",
          getFieldValueFeatureClassName(),
          fstore,
          "{\"field\":\"" + field + "\",\"defaultValue\":\"42.0\"}");

      SolrQuery query = new SolrQuery();
      query.setQuery("id:42");
      query.add("fl", "*, score");
      query.add("rows", "4");

      loadModel(
          field + "-model42",
          LinearModel.class.getName(),
          new String[] {field + "42"},
          fstore,
          "{\"weights\":{\"" + field + "42\":1.0}}");

      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ("/query" + query.toQueryString(), "/response/docs/[0]/id=='42'");

      final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(field + "42", "42.0");
      final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

      final String docs0fv_default_csv =
          chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

      query = new SolrQuery();
      query.setQuery("id:42");
      query.add("rq", "{!ltr model=" + field + "-model42 reRankDocs=4}");
      query.add("fl", "[fv]");

      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ(
          "/query" + query.toQueryString(),
          "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
    }
  }

  @Test
  public void testIfADocumentDoesntHaveAFieldTheDefaultValueFromSchemaIsReturned()
      throws Exception {
    final String[] fieldsWithDefaultValues = {"dvIntField", "dvLongField", "dvFloatField"};
    final String[] defaultValues = {"-1.0", "-2.0", "-3.0"};

    for (int idx = 0; idx < fieldsWithDefaultValues.length; ++idx) {
      final String field = fieldsWithDefaultValues[idx];
      final String defaultValue = defaultValues[idx];

      final String fstore =
          "testIfADocumentDoesntHaveAFieldTheDefaultValueFromSchemaIsReturned" + field;

      assertU(adoc("id", "21"));
      assertU(commit());

      loadFeature(field, getFieldValueFeatureClassName(), fstore, "{\"field\":\"" + field + "\"}");
      loadModel(
          field + "-model",
          LinearModel.class.getName(),
          new String[] {field},
          fstore,
          "{\"weights\":{\"" + field + "\":1.0}}");

      final SolrQuery query = new SolrQuery("id:21");
      query.add("rq", "{!ltr model=" + field + "-model reRankDocs=4}");
      query.add("fl", "[fv]");

      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ(
          "/query" + query.toQueryString(),
          "/response/docs/[0]/=={'[fv]':'"
              + FeatureLoggerTestUtils.toFeatureVector(field, defaultValue)
              + "'}");
    }
  }

  @Test
  public void testThatFieldValueFeatureScorerIsUsedAndDefaultIsReturned() throws Exception {
    // this tests the case that we create a feature for a non-existent field
    // using a different fstore to avoid a clash with the other tests
    final String fstore = "testThatFieldValueFeatureScorerIsUsedAndDefaultIsReturned";
    loadFeature(
        "not-existing-field",
        getObservingFieldValueFeatureClassName(),
        fstore,
        "{\"field\":\"cowabunga\"}");

    loadModel(
        "not-existing-field-model",
        LinearModel.class.getName(),
        new String[] {"not-existing-field"},
        fstore,
        "{\"weights\":{\"not-existing-field\":1.0}}");

    final String docs0fv_dense_csv =
        FeatureLoggerTestUtils.toFeatureVector(
            "not-existing-field", Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL));
    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    final SolrQuery query = new SolrQuery();
    query.setQuery("id:42");
    query.add("rq", "{!ltr model=not-existing-field-model reRankDocs=4}");
    query.add("fl", "[fv]");
    ObservingFieldValueFeature.usedScorerClass = null; // to clear away any previous test's use
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
    assertEquals(
        FieldValueFeatureScorer.class.getName(), ObservingFieldValueFeature.usedScorerClass);
  }

  @Test
  public void testThatDefaultFieldValueScorerIsUsedAndDefaultIsReturned() throws Exception {
    final String[] fieldsWithoutDefaultValues = {"dvDoubleField", "dvStrBoolField"};
    // this tests the case that no document contains docValues for the provided existing field

    for (String field : fieldsWithoutDefaultValues) {
      final String fstore = "testThatDefaultFieldValueScorerIsUsedAndDefaultIsReturned" + field;

      loadFeature(
          field, getObservingFieldValueFeatureClassName(), fstore, "{\"field\":\"" + field + "\"}");

      loadModel(
          field + "-model",
          LinearModel.class.getName(),
          new String[] {field},
          fstore,
          "{\"weights\":{\"" + field + "\":1.0}}");

      final String docs0fv_dense_csv =
          FeatureLoggerTestUtils.toFeatureVector(
              field, Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL));
      final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

      final String docs0fv_default_csv =
          chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

      final SolrQuery query = new SolrQuery("id:42");
      query.add("rq", "{!ltr model=" + field + "-model reRankDocs=4}");
      query.add("fl", "[fv]");

      ObservingFieldValueFeature.usedScorerClass = null; // to clear away any previous test's use
      assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
      assertJQ(
          "/query" + query.toQueryString(),
          "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
      assertEquals(
          DefaultValueFieldValueFeatureScorer.class.getName(),
          ObservingFieldValueFeature.usedScorerClass);
    }
  }

  @Test
  public void testBooleanValue() throws Exception {
    implTestBooleanValue("isTrendy");
  }

  @Test
  public void testBooleanValue_docValues() throws Exception {
    implTestBooleanValue("dvIsTrendy");
  }

  @Test
  public void testBooleanValue_stored_docValues() throws Exception {
    implTestBooleanValue("storedDvIsTrendy");
  }

  private void implTestBooleanValue(String isTrendyFieldName) throws Exception {
    final String fstore = "test_boolean_store";
    loadFeature(
        "trendy",
        getFieldValueFeatureClassName(),
        fstore,
        "{\"field\":\"" + isTrendyFieldName + "\"}");

    loadModel(
        "trendy-model",
        LinearModel.class.getName(),
        new String[] {"trendy"},
        fstore,
        "{\"weights\":{\"trendy\":1.0}}");

    final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector("trendy", "0.0");
    final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

    final String docs0fv_default_csv =
        chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

    SolrQuery query = new SolrQuery();
    query.setQuery("id:4");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");

    query = new SolrQuery();
    query.setQuery("id:5");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'"
            + FeatureLoggerTestUtils.toFeatureVector("trendy", "1.0")
            + "'}");

    // check default value is false
    query = new SolrQuery();
    query.setQuery("id:2");
    query.add("rq", "{!ltr model=trendy-model reRankDocs=4}");
    query.add("fl", "[fv]");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}");
  }

  @Test
  public void testThatExceptionIsThrownForUnsupportedType() throws Exception {
    final String fstore = "test_store";

    assertU(
        adoc(
            "id", "21", "title", "multivalued not supported", "dvStringPopularities", "wow value"));
    assertU(commit());

    loadFeature(
        "dvStringPopularities",
        getFieldValueFeatureClassName(),
        fstore,
        "{\"field\":\"dvStringPopularities\"}");

    loadModel(
        "dvStringPopularities-model",
        LinearModel.class.getName(),
        new String[] {"dvStringPopularities"},
        fstore,
        "{\"weights\":{\"dvStringPopularities\":1.0}}");

    final SolrQuery query = new SolrQuery("id:21");
    query.add("rq", "{!ltr model=dvStringPopularities-model reRankDocs=4}");
    query.add("fl", "[fv]");

    assertJQ(
        "/query" + query.toQueryString(),
        "/error/msg/=='java.lang.IllegalArgumentException: Doc values type SORTED_SET of field dvStringPopularities is not supported'");
  }

  protected String storedDvIsTrendy_FieldValueFeatureScorer_className() {
    return SortedDocValuesFieldValueFeatureScorer.class.getName();
  }

  @Test
  public void testThatCorrectFieldValueFeatureIsUsedForDocValueTypes() throws Exception {
    final String[][] fieldsWithDifferentTypes = {
      new String[] {
        "dvIntPopularity", "1", NumericDocValuesFieldValueFeatureScorer.class.getName()
      },
      new String[] {
        "dvStringPopularity", "T", SortedDocValuesFieldValueFeatureScorer.class.getName()
      },
      new String[] {"dvIsTrendy", "1", SortedDocValuesFieldValueFeatureScorer.class.getName()},
      new String[] {"storedDvIsTrendy", "1", storedDvIsTrendy_FieldValueFeatureScorer_className()},
      new String[] {"noDvFloatField", "1", FieldValueFeatureScorer.class.getName()},
      new String[] {"noDvStrNumField", "T", FieldValueFeatureScorer.class.getName()}
    };

    for (String[] fieldAndScorerClass : fieldsWithDifferentTypes) {
      final String field = fieldAndScorerClass[0];
      final String fieldValue = fieldAndScorerClass[1];
      final String fstore = "testThatCorrectFieldValueFeatureIsUsedForDocValueTypes" + field;
      final String modelName = field + "-model";

      loadFeatureAndModel(getObservingFieldValueFeatureClassName(), field, fstore, modelName);

      final String usedScorerClass = addAndQueryId21(field, modelName, fieldValue);

      assertEquals(fieldAndScorerClass[2], usedScorerClass);
    }
  }

  protected void loadFeatureAndModel(
      String featureClassName, String field, String fstore, String modelName) throws Exception {
    loadFeature(field, featureClassName, fstore, "{\"field\":\"" + field + "\"}");

    loadModel(
        modelName,
        LinearModel.class.getName(),
        new String[] {field},
        fstore,
        "{\"weights\":{\"" + field + "\":1.0}}");
  }

  /**
   * @return used scorer class
   */
  protected String addAndQueryId21(String field, String modelName, String fieldValue)
      throws Exception {

    assertU(adoc("id", "21", field, fieldValue));
    assertU(commit());

    final SolrQuery query = new SolrQuery("id:21");
    query.add("rq", "{!ltr model=" + modelName + " reRankDocs=4}");
    query.add("fl", "[fv]");

    ObservingFieldValueFeature.usedScorerClass = null; // to clear away any previous test's use
    assertJQ("/query" + query.toQueryString(), "/response/numFound/==1");
    assertJQ(
        "/query" + query.toQueryString(),
        "/response/docs/[0]/=={'[fv]':'"
            + FeatureLoggerTestUtils.toFeatureVector(field, "1.0")
            + "'}");
    return ObservingFieldValueFeature.usedScorerClass;
  }

  @Test
  public void testParamsToMap() throws Exception {
    final LinkedHashMap<String, Object> params = new LinkedHashMap<>();
    params.put("field", "field" + random().nextInt(10));
    doTestParamsToMap(FieldValueFeature.class.getName(), params);
  }

  @Test
  public void testThatStringValuesAreCorrectlyParsed() throws Exception {
    for (String field : new String[] {"dvStrNumField", "noDvStrNumField"}) {
      final String false_docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(field, "0.0");
      final String default_docs0fv_dense_csv =
          FeatureLoggerTestUtils.toFeatureVector(
              field, Float.toString(FIELD_VALUE_FEATURE_DEFAULT_VAL));
      final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

      final String false_docs0fv_default_csv =
          chooseDefaultFeatureVector(false_docs0fv_dense_csv, docs0fv_sparse_csv);
      final String default_docs0fv_default_csv =
          chooseDefaultFeatureVector(default_docs0fv_dense_csv, docs0fv_sparse_csv);

      final String[][] inputsAndTests = {
        new String[] {
          "T",
          "/response/docs/[0]/=={'[fv]':'"
              + FeatureLoggerTestUtils.toFeatureVector(field, "1.0")
              + "'}"
        },
        new String[] {"F", "/response/docs/[0]/=={'[fv]':'" + false_docs0fv_default_csv + "'}"},
        new String[] {
          "-7324.427", "/response/docs/[0]/=={'[fv]':'" + default_docs0fv_default_csv + "'}"
        },
        new String[] {"532", "/response/docs/[0]/=={'[fv]':'" + default_docs0fv_default_csv + "'}"},
        new String[] {
          Float.toString(Float.NaN),
          "/response/docs/[0]/=={'[fv]':'" + default_docs0fv_default_csv + "'}"
        },
        new String[] {
          "notanumber", "/response/docs/[0]/=={'[fv]':'" + default_docs0fv_default_csv + "'}"
        }
      };

      final String fstore = "testThatStringValuesAreCorrectlyParsed" + field;
      loadFeature(field, getFieldValueFeatureClassName(), fstore, "{\"field\":\"" + field + "\"}");
      loadModel(
          field + "-model",
          LinearModel.class.getName(),
          new String[] {field},
          fstore,
          "{\"weights\":{\"" + field + "\":1.0}}");

      for (String[] inputAndTest : inputsAndTests) {
        assertU(adoc("id", "21", field, inputAndTest[0]));
        assertU(commit());

        final SolrQuery query = new SolrQuery("id:21");
        query.add("rq", "{!ltr model=" + field + "-model reRankDocs=4}");
        query.add("fl", "[fv]");

        assertJQ("/query" + query.toQueryString(), inputAndTest[1]);
      }
    }
  }

  @Test
  public void testThatDateValuesAreCorrectlyParsed() throws Exception {
    for (String field : new String[] {"dvDateField", "noDvDateField"}) {
      final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(field, "0.0");
      final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

      final String docs0fv_default_csv =
          chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

      final String[][] inputsAndTests = {
        new String[] {
          "1970-01-01T00:00:00.000Z", "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}"
        },
        new String[] {
          "1970-01-01T00:00:00.001Z",
          "/response/docs/[0]/=={'[fv]':'"
              + FeatureLoggerTestUtils.toFeatureVector(field, "1.0")
              + "'}"
        },
        new String[] {
          "1970-01-01T00:00:01.234Z",
          "/response/docs/[0]/=={'[fv]':'"
              + FeatureLoggerTestUtils.toFeatureVector(field, "1234.0")
              + "'}"
        }
      };

      final String fstore = "testThatDateValuesAreCorrectlyParsed" + field;
      loadFeature(field, getFieldValueFeatureClassName(), fstore, "{\"field\":\"" + field + "\"}");
      loadModel(
          field + "-model",
          LinearModel.class.getName(),
          new String[] {field},
          fstore,
          "{\"weights\":{\"" + field + "\":1.0}}");

      for (String[] inputAndTest : inputsAndTests) {
        assertU(adoc("id", "21", field, inputAndTest[0]));
        assertU(commit());

        final SolrQuery query = new SolrQuery("id:21");
        query.add("rq", "{!ltr model=" + field + "-model reRankDocs=4}");
        query.add("fl", "[fv]");

        assertJQ("/query" + query.toQueryString(), inputAndTest[1]);
      }
    }
  }

  public static class RelativeDateFieldValueFeature extends FieldValueFeature {

    private boolean since = false;
    private boolean until = false;

    public boolean getSince() {
      return this.since;
    }

    public void setSince(boolean since) {
      this.since = since;
    }

    public boolean getUntil() {
      return this.until;
    }

    public void setUntil(boolean until) {
      this.until = until;
    }

    public RelativeDateFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    @Override
    protected void validate() throws FeatureException {
      if (since != until) {
        return;
      }
      throw new FeatureException(
          getClass().getSimpleName() + ": exactly one of 'since' and 'until' must be provided");
    }

    @Override
    public FeatureWeight createWeight(
        IndexSearcher searcher,
        boolean needsScores,
        SolrQueryRequest request,
        Query originalQuery,
        Map<String, String[]> efi)
        throws IOException {
      return new FieldValueFeatureWeight(searcher, request, originalQuery, efi) {
        private final long timeZero = Instant.parse("2000-01-01T00:00:00.000Z").toEpochMilli();

        @Override
        public long readNumericDocValuesDate(long val) {
          if (since) return TimeUnit.MILLISECONDS.toMinutes(val - this.timeZero);
          if (until) return TimeUnit.MILLISECONDS.toMinutes(this.timeZero - val);
          return 0;
        }
      };
    }
  }

  @Test
  public void testRelativeDateFieldValueFeature() throws Exception {
    final String field = "dvDateField";
    for (boolean since : new boolean[] {false, true}) {
      final String docs0fv_dense_csv = FeatureLoggerTestUtils.toFeatureVector(field, "0.0");
      final String docs0fv_sparse_csv = FeatureLoggerTestUtils.toFeatureVector("");

      final String docs0fv_default_csv =
          chooseDefaultFeatureVector(docs0fv_dense_csv, docs0fv_sparse_csv);

      final String[][] inputsAndTests = {
        new String[] {
          "2000-01-01T00:00:00.000Z", "/response/docs/[0]/=={'[fv]':'" + docs0fv_default_csv + "'}"
        },
        new String[] {
          "2000-01-01T00:01:02.003Z",
          "/response/docs/[0]/=={'[fv]':'"
              + FeatureLoggerTestUtils.toFeatureVector(field, (since ? "1.0" : "-1.0"))
              + "'}"
        },
        new String[] {
          "2000-01-01T01:02:03.004Z",
          "/response/docs/[0]/=={'[fv]':'"
              + FeatureLoggerTestUtils.toFeatureVector(field, (since ? "62.0" : "-62.0"))
              + "'}"
        }
      };

      final String fstore = "testRelativeDateFieldValueFeature" + field + "_" + since;
      final String model = fstore + "-model";
      loadFeature(
          field,
          RelativeDateFieldValueFeature.class.getName(),
          fstore,
          "{\"field\":\"" + field + "\", \"" + (since ? "since" : "until") + "\": true}");
      loadModel(
          model,
          LinearModel.class.getName(),
          new String[] {field},
          fstore,
          "{\"weights\":{\"" + field + "\":1.0}}");

      for (String[] inputAndTest : inputsAndTests) {
        assertU(adoc("id", "21", field, inputAndTest[0]));
        assertU(commit());

        final SolrQuery query = new SolrQuery("id:21");
        query.add("rq", "{!ltr model=" + model + " reRankDocs=4}");
        query.add("fl", "[fv]");

        assertJQ("/query" + query.toQueryString(), inputAndTest[1]);
      }
    }
  }

  /**
   * This class is used to track which specific FieldValueFeature is used so that we can test,
   * whether the fallback mechanism works correctly.
   */
  public static class ObservingFieldValueFeature extends FieldValueFeature {
    static String usedScorerClass;

    public ObservingFieldValueFeature(String name, Map<String, Object> params) {
      super(name, params);
    }

    @Override
    public Feature.FeatureWeight createWeight(
        IndexSearcher searcher,
        boolean needsScores,
        SolrQueryRequest request,
        Query originalQuery,
        Map<String, String[]> efi)
        throws IOException {
      return new ObservingFieldValueFeatureWeight(searcher, request, originalQuery, efi);
    }

    public class ObservingFieldValueFeatureWeight extends FieldValueFeatureWeight {
      public ObservingFieldValueFeatureWeight(
          IndexSearcher searcher,
          SolrQueryRequest request,
          Query originalQuery,
          Map<String, String[]> efi) {
        super(searcher, request, originalQuery, efi);
      }

      @Override
      public FeatureScorer scorer(LeafReaderContext context) throws IOException {
        FeatureScorer scorer = super.scorer(context);
        usedScorerClass = scorer.getClass().getName();
        return scorer;
      }
    }
  }
}
