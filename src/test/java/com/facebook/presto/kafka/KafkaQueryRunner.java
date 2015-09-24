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

import com.facebook.presto.kafka.util.TestUtils;
import com.facebook.presto.kafka.util.EmbeddedKafka;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.tests.TestingPrestoClient;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;

import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static io.airlift.units.Duration.nanosSince;
import static java.util.Locale.ENGLISH;

public final class KafkaQueryRunner
{
    private KafkaQueryRunner()
    {
    }

    private static final Logger log = Logger.get("TestQueries");
    private static final String TPCH_SCHEMA = "tpch";

    private static void loadTpchTopic(EmbeddedKafka embeddedKafka, TestingPrestoClient prestoClient, TpchTable<?> table)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        TestUtils.loadTpchTopic(embeddedKafka, prestoClient, kafkaTopicName(table), new QualifiedTableName("tpch", TINY_SCHEMA_NAME, table.getTableName().toLowerCase(ENGLISH)));
        log.info("Imported %s in %s", 0, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String kafkaTopicName(TpchTable<?> table)
    {
        return TPCH_SCHEMA + "." + table.getTableName().toLowerCase(ENGLISH);
    }
}
