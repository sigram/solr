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

package org.apache.solr.cluster.api;

/** A shard of a collection */
@Deprecated
public interface Shard {

  /** name of the shard */
  String name();

  /** collection this shard belongs to */
  String collection();

  /** hash range of this shard. null if this is not using hash based router */
  HashRange range();

  /** replicas of the shard */
  SimpleMap<ShardReplica> replicas();

  /** Name of the replica that is acting as the leader at the moment */
  String leader();
}
