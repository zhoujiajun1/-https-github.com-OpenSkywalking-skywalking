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

package org.apache.skywalking.library.elasticsearch.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.linecorp.armeria.client.WebClient;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpStatus;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.skywalking.library.elasticsearch.requests.factory.RequestFactory;
import org.apache.skywalking.library.elasticsearch.response.Index;
import org.apache.skywalking.library.elasticsearch.response.Mappings;

@Slf4j
@RequiredArgsConstructor
public final class IndexClient {
    private final CompletableFuture<RequestFactory> requestFactory;

    private final WebClient client;

    @SneakyThrows
    public boolean exists(String name) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.index().exists(name))
                        .aggregate().thenApply(response -> response.status() == HttpStatus.OK)
                        .exceptionally(e -> {
                            log.error("Failed to check whether index exists", e);
                            return false;
                        })).get();
    }

    @SneakyThrows
    public Optional<Index> get(String name) {
        final TypeReference<Map<String, Index>> type =
            new TypeReference<Map<String, Index>>() {
            };
        final CompletableFuture<Optional<Index>> future = requestFactory.thenCompose(
            rf -> client.execute(rf.index().get(name))
                        .aggregate().thenApply(response -> {
                    final HttpStatus status = response.status();
                    if (status == HttpStatus.NOT_FOUND) {
                        return Optional.empty();
                    }

                    try (final HttpData content = response.content();
                         final InputStream is = content.toInputStream()) {
                        final Map<String, Index> indices = rf.codec().decode(is, type);
                        return Optional.ofNullable(indices.get(name));
                    } catch (Exception e) {
                        log.error("Failed to close input stream", e);
                        return Optional.empty();
                    }
                }));

        return future.get();
    }

    @SneakyThrows
    public boolean create(String name,
                          Mappings mappings,
                          Map<String, ?> settings) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.index().create(name, mappings, settings))
                        .aggregate().thenApply(response -> response.status() == HttpStatus.OK)
                        .exceptionally(e -> {
                            log.error("Failed to check whether index exists", e);
                            return false;
                        })).get();
    }

    @SneakyThrows
    public boolean delete(String name) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.index().delete(name))
                        .aggregate().thenApply(response -> response.status() == HttpStatus.OK)
                        .exceptionally(e -> {
                            log.error("Failed to delete whether index exists", e);
                            return false;
                        })).get();
    }

    @SneakyThrows
    public boolean putMapping(String name, String type, Mappings mapping) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.index().putMapping(name, type, mapping))
                        .aggregate().thenApply(response -> {
                    if (response.status() == HttpStatus.OK) {
                        return true;
                    }
                    log.error(
                        "Failed to update index mapping.",
                        new RuntimeException(response.contentUtf8())
                    );
                    return false;
                }).exceptionally(e -> {
                    log.error("Failed to update index mapping", e);
                    return false;
                })).get();
    }
}
