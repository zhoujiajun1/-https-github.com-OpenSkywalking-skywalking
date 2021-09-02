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
 *
 */

package org.apache.skywalking.oap.server.library.client.elasticsearch;

import com.google.common.collect.ImmutableMap;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantLock;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.apm.util.StringUtil;
import org.apache.skywalking.library.elasticsearch.bulk.BulkProcessor;
import org.apache.skywalking.library.elasticsearch.requests.search.Query;
import org.apache.skywalking.library.elasticsearch.requests.search.Search;
import org.apache.skywalking.library.elasticsearch.response.Document;
import org.apache.skywalking.library.elasticsearch.response.Documents;
import org.apache.skywalking.library.elasticsearch.response.Index;
import org.apache.skywalking.library.elasticsearch.response.IndexTemplate;
import org.apache.skywalking.library.elasticsearch.response.Mappings;
import org.apache.skywalking.library.elasticsearch.response.search.SearchResponse;
import org.apache.skywalking.oap.server.library.client.Client;
import org.apache.skywalking.oap.server.library.client.healthcheck.DelegatedHealthChecker;
import org.apache.skywalking.oap.server.library.client.healthcheck.HealthCheckable;
import org.apache.skywalking.oap.server.library.util.HealthChecker;

/**
 * ElasticSearchClient connects to the ES server by using ES client APIs.
 */
@Slf4j
@RequiredArgsConstructor
public class ElasticSearchClient implements Client, HealthCheckable {
    public static final String TYPE = "type";

    protected final String clusterNodes;

    protected final String protocol;

    private final String trustStorePath;

    @Setter
    private volatile String trustStorePass;

    @Setter
    private volatile String user;

    @Setter
    private volatile String password;

    private final List<IndexNameConverter> indexNameConverters;

    protected DelegatedHealthChecker healthChecker = new DelegatedHealthChecker();

    protected final ReentrantLock connectLock = new ReentrantLock();

    private final int connectTimeout;

    private final int socketTimeout;

    org.apache.skywalking.library.elasticsearch.ElasticSearchClient esClient;

    public ElasticSearchClient(String clusterNodes,
                               String protocol,
                               String trustStorePath,
                               String trustStorePass,
                               String user,
                               String password,
                               List<IndexNameConverter> indexNameConverters,
                               int connectTimeout,
                               int socketTimeout) {
        this.clusterNodes = clusterNodes;
        this.protocol = protocol;
        this.trustStorePath = trustStorePath;
        this.trustStorePass = trustStorePass;
        this.user = user;
        this.password = password;
        this.indexNameConverters = indexNameConverters;
        this.connectTimeout = connectTimeout;
        this.socketTimeout = socketTimeout;
    }

    @Override
    public void connect() {
        connectLock.lock();
        try {
            if (esClient != null) {
                try {
                    esClient.close();
                } catch (Throwable t) {
                    log.error("ElasticSearch client reconnection fails based on new config", t);
                }
            }
            esClient = org.apache.skywalking.library.elasticsearch.ElasticSearchClient
                .builder()
                .endpoints(clusterNodes.split(","))
                .protocol(protocol)
                .trustStorePath(trustStorePath)
                .trustStorePass(trustStorePass)
                .username(user)
                .password(password)
                .connectTimeout(connectTimeout)
                .build();
            esClient.connect();
        } finally {
            connectLock.unlock();
        }
    }

    @Override
    public void shutdown() {
        esClient.close();
    }

    public boolean createIndex(String indexName) {
        return createIndex(indexName, null, null);
    }

    public boolean createIndex(String indexName,
                               Mappings mappings,
                               Map<String, ?> settings) {
        indexName = formatIndexName(indexName);

        return esClient.index().create(indexName, mappings, settings);
    }

    public boolean updateIndexMapping(String indexName, Mappings mapping) {
        indexName = formatIndexName(indexName);

        return esClient.index().putMapping(indexName, TYPE, mapping);
    }

    public Optional<Index> getIndex(String indexName) {
        if (StringUtil.isBlank(indexName)) {
            return Optional.empty();
        }
        indexName = formatIndexName(indexName);
        try {
            return esClient.index().get(indexName);
        } catch (Exception t) {
            healthChecker.unHealth(t);
            throw t;
        }
    }

    public Collection<String> retrievalIndexByAliases(String aliases) {
        aliases = formatIndexName(aliases);

        return esClient.alias().indices(aliases).keySet();
    }

    /**
     * If your indexName is retrieved from elasticsearch through {@link
     * #retrievalIndexByAliases(String)} or some other method and it already contains namespace.
     * Then you should delete the index by this method, this method will no longer concatenate
     * namespace.
     * <p>
     * https://github.com/apache/skywalking/pull/3017
     */
    public boolean deleteByIndexName(String indexName) {
        return deleteIndex(indexName, false);
    }

    /**
     * If your indexName is obtained from metadata or configuration and without namespace. Then you
     * should delete the index by this method, this method automatically concatenates namespace.
     * <p>
     * https://github.com/apache/skywalking/pull/3017
     */
    public boolean deleteByModelName(String modelName) {
        return deleteIndex(modelName, true);
    }

    protected boolean deleteIndex(String indexName, boolean formatIndexName) {
        if (formatIndexName) {
            indexName = formatIndexName(indexName);
        }
        return esClient.index().delete(indexName);
    }

    public boolean isExistsIndex(String indexName) {
        indexName = formatIndexName(indexName);

        return esClient.index().exists(indexName);
    }

    public Optional<IndexTemplate> getTemplate(String name) {
        name = formatIndexName(name);

        try {
            return esClient.templates().get(name);
        } catch (Exception e) {
            healthChecker.unHealth(e);
            throw e;
        }
    }

    public boolean isExistsTemplate(String indexName) {
        indexName = formatIndexName(indexName);

        return esClient.templates().exists(indexName);
    }

    public boolean createOrUpdateTemplate(String indexName, Map<String, Object> settings,
                                          Mappings mapping, int order) {
        indexName = formatIndexName(indexName);

        return esClient.templates().createOrUpdate(indexName, settings, mapping, order);
    }

    public boolean deleteTemplate(String indexName) {
        indexName = formatIndexName(indexName);

        return esClient.templates().delete(indexName);
    }

    public SearchResponse search(IndexNameMaker indexNameMaker, Search search) {
        final String[] indexNames =
            Arrays.stream(indexNameMaker.make())
                  .map(this::formatIndexName)
                  .toArray(String[]::new);
        return doSearch(search, indexNames);
    }

    public SearchResponse search(String indexName, Search search) {
        indexName = formatIndexName(indexName);
        return esClient.search(search, indexName);
    }

    protected SearchResponse doSearch(Search search, String... indexNames) {
        return esClient.search(
            search,
            ImmutableMap.of(
                "ignore_unavailable", true,
                "allow_no_indices", true,
                "expand_wildcards", "open"
            ),
            indexNames
        );
    }

    public Optional<Document> get(String indexName, String id) {
        indexName = formatIndexName(indexName);

        return esClient.documents().get(indexName, TYPE, id);
    }

    public boolean existDoc(String indexName, String id) {
        indexName = formatIndexName(indexName);

        return esClient.documents().exists(indexName, TYPE, id);
    }

    public Optional<Documents> ids(String indexName, Iterable<String> ids) {
        indexName = formatIndexName(indexName);

        return esClient.documents().mget(indexName, TYPE, ids);
    }

    public Optional<Documents> ids(String indexName, String[] ids) {
        return ids(indexName, Arrays.asList(ids));
    }

    public void forceInsert(String indexName, String id, Map<String, Object> source) {
        IndexRequestWrapper wrapper = prepareInsert(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        try {
            esClient.documents().index(wrapper.getRequest(), params);
            healthChecker.health();
        } catch (Throwable t) {
            healthChecker.unHealth(t);
            throw t;
        }
    }

    public void forceUpdate(String indexName, String id, Map<String, Object> source) {
        UpdateRequestWrapper wrapper = prepareUpdate(indexName, id, source);
        Map<String, Object> params = ImmutableMap.of("refresh", "true");
        try {
            esClient.documents().update(wrapper.getRequest(), params);
            healthChecker.health();
        } catch (Throwable t) {
            healthChecker.unHealth(t);
            throw t;
        }
    }

    public IndexRequestWrapper prepareInsert(String indexName, String id,
                                             Map<String, Object> source) {
        indexName = formatIndexName(indexName);
        return new IndexRequestWrapper(indexName, TYPE, id, source);
    }

    public UpdateRequestWrapper prepareUpdate(String indexName, String id,
                                              Map<String, Object> source) {
        indexName = formatIndexName(indexName);
        return new UpdateRequestWrapper(indexName, TYPE, id, source);
    }

    public void delete(String indexName, String timeBucketColumnName, long endTimeBucket) {
        indexName = formatIndexName(indexName);
        final Map<String, Object> params = Collections.singletonMap("conflicts", "proceed");
        final Query query = Query.range(timeBucketColumnName).lte(endTimeBucket).build();

        esClient.documents().delete(indexName, TYPE, query, params);
    }

    public BulkProcessor createBulkProcessor(int bulkActions,
                                             int flushInterval,
                                             int concurrentRequests) {
        return esClient.bulkProcessor()
                       .bulkActions(bulkActions)
                       .flushInterval(Duration.ofSeconds(flushInterval))
                       .concurrentRequests(concurrentRequests)
                       .build();
    }

    public String formatIndexName(String indexName) {
        for (final IndexNameConverter indexNameConverter : indexNameConverters) {
            indexName = indexNameConverter.convert(indexName);
        }
        return indexName;
    }

    @Override
    public void registerChecker(HealthChecker healthChecker) {
        this.healthChecker.register(healthChecker);
    }
}
