/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.kafka;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.eventbus.EventBus;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.name.Names;
import org.rakam.analysis.JDBCMetastore;
import org.rakam.collection.event.FieldDependencyBuilder;
import org.rakam.collection.event.metastore.Metastore;
import org.rakam.plugin.JDBCConfig;
import org.rakam.report.PrestoConfig;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class MetastoreModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(JDBCConfig.class,
                Names.named("presto.metastore.jdbc"), "presto.metastore.jdbc");
        binder.bind(EventBus.class).in(Scopes.SINGLETON);
        binder.bind(FieldDependencyBuilder.FieldDependency.class)
                .toInstance(new FieldDependencyBuilder.FieldDependency(ImmutableSet.of(), ImmutableMap.of()));
        binder.bind(Metastore.class).to(JDBCMetastore.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(PrestoConfig.class);
    }
}
