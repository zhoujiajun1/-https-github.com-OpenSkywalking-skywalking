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
import org.apache.skywalking.library.elasticsearch.response.IndexTemplate;
import org.apache.skywalking.library.elasticsearch.response.IndexTemplates;
import org.apache.skywalking.library.elasticsearch.response.Mappings;

@Slf4j
@RequiredArgsConstructor
public final class TemplateClient {
    private final CompletableFuture<RequestFactory> requestFactory;

    private final WebClient client;

    @SneakyThrows
    public boolean exists(String name) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.template().exists(name))
                        .aggregate().thenApply(response -> {
                    final HttpStatus status = response.status();
                    if (status == HttpStatus.OK) {
                        return true;
                    } else if (status == HttpStatus.NOT_FOUND) {
                        return false;
                    }
                    throw new RuntimeException(
                        "Response status code of template exists request should be 200 or 404," +
                            " but it was: " + status.code());
                }).exceptionally(e -> {
                    log.error("Failed to check whether template exists", e);
                    return false;
                })).get();
    }

    @SneakyThrows
    public Optional<IndexTemplate> get(String name) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.template().get(name))
                        .aggregate().thenApply(response -> {
                    final HttpStatus status = response.status();
                    if (status != HttpStatus.OK) {
                        return Optional.<IndexTemplate>empty();
                    }

                    try (final HttpData content = response.content();
                         final InputStream is = content.toInputStream()) {
                        final IndexTemplates templates =
                            rf.codec().decode(is, IndexTemplates.class);
                        return templates.get(name);
                    } catch (Exception e) {
                        log.error("Failed to close input stream", e);
                        return Optional.<IndexTemplate>empty();
                    }
                }).exceptionally(e -> {
                    log.error("Failed to check whether template exists", e);
                    return Optional.empty();
                })).get();
    }

    @SneakyThrows
    public boolean delete(String name) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.template().delete(name))
                        .aggregate().thenApply(response -> {
                    if (response.status() == HttpStatus.OK) {
                        return true;
                    }
                    log.error(
                        "Failed to check whether template exists. {}",
                        response.contentUtf8()
                    );
                    return false;
                }).exceptionally(e -> {
                    log.error("Failed to check whether template exists", e);
                    return false;
                })).get();
    }

    @SneakyThrows
    public boolean createOrUpdate(String name, Map<String, Object> settings,
                                  Mappings mappings, int order) {
        return requestFactory.thenCompose(
            rf -> client.execute(rf.template().createOrUpdate(name, settings, mappings, order))
                        .aggregate().thenApply(response -> {
                    if (response.status() == HttpStatus.OK) {
                        return true;
                    }
                    log.error(
                        "Failed to create or update template.",
                        new RuntimeException(response.contentUtf8())
                    );
                    return false;
                }).exceptionally(e -> {
                    log.error("Failed to create or update template", e);
                    return false;
                })).get();
    }

}
