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
package org.apache.solr.cloud;

import static org.apache.solr.common.params.CommonParams.ID;

import com.codahale.metrics.Timer;
import java.io.Closeable;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import org.apache.lucene.util.Version;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.cloud.SolrCloudManager;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.cloud.api.collections.CreateCollectionCmd;
import org.apache.solr.cloud.api.collections.OverseerCollectionMessageHandler;
import org.apache.solr.cloud.overseer.ClusterStateMutator;
import org.apache.solr.cloud.overseer.CollectionMutator;
import org.apache.solr.cloud.overseer.NodeMutator;
import org.apache.solr.cloud.overseer.OverseerAction;
import org.apache.solr.cloud.overseer.ReplicaMutator;
import org.apache.solr.cloud.overseer.SliceMutator;
import org.apache.solr.cloud.overseer.ZkStateWriter;
import org.apache.solr.cloud.overseer.ZkWriteCommand;
import org.apache.solr.common.AlreadyClosedException;
import org.apache.solr.common.MapWriter;
import org.apache.solr.common.SolrCloseable;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.cloud.ClusterState;
import org.apache.solr.common.cloud.DocCollection;
import org.apache.solr.common.cloud.Slice;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkNodeProps;
import org.apache.solr.common.cloud.ZkStateReader;
import org.apache.solr.common.params.CollectionAdminParams;
import org.apache.solr.common.params.CollectionParams;
import org.apache.solr.common.util.Compressor;
import org.apache.solr.common.util.IOUtils;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.ObjectReleaseTracker;
import org.apache.solr.common.util.Pair;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.common.util.Utils;
import org.apache.solr.common.util.ZLibCompressor;
import org.apache.solr.core.CloudConfig;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrInfoBean;
import org.apache.solr.handler.admin.CollectionsHandler;
import org.apache.solr.handler.component.HttpShardHandler;
import org.apache.solr.logging.MDCLoggingContext;
import org.apache.solr.metrics.SolrMetricProducer;
import org.apache.solr.metrics.SolrMetricsContext;
import org.apache.solr.update.UpdateShardHandler;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cluster leader. Responsible for processing state updates, node assignments, creating/deleting
 * collections, shards, replicas and setting various properties.
 *
 * <p>The <b>Overseer</b> is a single elected node in the SolrCloud cluster that is in charge of
 * interactions with ZooKeeper that require global synchronization.
 *
 * <p>The Overseer deals with:
 *
 * <ul>
 *   <li>Cluster State updates, i.e. updating Collections' <code>state.json</code> files in
 *       ZooKeeper, see {@link ClusterStateUpdater},
 *   <li>Collection API implementation, see {@link OverseerCollectionConfigSetProcessor} and {@link
 *       OverseerCollectionMessageHandler} (and the example below),
 *   <li>Updating Config Sets, see {@link OverseerCollectionConfigSetProcessor} and {@link
 *       OverseerConfigSetMessageHandler},
 * </ul>
 *
 * <p>The nodes in the cluster communicate with the Overseer over queues implemented in ZooKeeper.
 * There are essentially two queues:
 *
 * <ol>
 *   <li>The <b>state update queue</b>, through which nodes request the Overseer to update the
 *       <code>state.json</code> file of a Collection in ZooKeeper. This queue is in Zookeeper at
 *       <code>/overseer/queue</code>,
 *   <li>A queue shared between <b>Collection API and Config Set API</b> requests. This queue is in
 *       Zookeeper at <code>/overseer/collection-queue-work</code>.
 * </ol>
 *
 * <p>An example of the steps involved in the Overseer processing a Collection creation API call:
 *
 * <ol>
 *   <li>Client uses the Collection API with <code>CREATE</code> action and reaches a node of the
 *       cluster,
 *   <li>The node (via {@link CollectionsHandler}) enqueues the request into the <code>
 *       /overseer/collection-queue-work</code> queue in ZooKeepeer,
 *   <li>The {@link OverseerCollectionConfigSetProcessor} running on the Overseer node dequeues the
 *       message and using an executor service with a maximum pool size of {@link
 *       OverseerTaskProcessor#MAX_PARALLEL_TASKS} hands it for processing to {@link
 *       OverseerCollectionMessageHandler},
 *   <li>Command {@link CreateCollectionCmd} then executes and does:
 *       <ol>
 *         <li>Update some state directly in ZooKeeper (creating collection znode),
 *         <li>Compute replica placement on available nodes in the cluster,
 *         <li>Enqueue a state change request for creating the <code>state.json</code> file for the
 *             collection in ZooKeeper. This is done by enqueuing a message in <code>/overseer/queue
 *             </code>,
 *         <li>The command then waits for the update to be seen in ZooKeeper...
 *       </ol>
 *   <li>The {@link ClusterStateUpdater} (also running on the Overseer node) dequeues the state
 *       change message and creates the <code>state.json</code> file in ZooKeeper for the
 *       Collection. All the work of the cluster state updater (creations, updates, deletes) is done
 *       sequentially for the whole cluster by a single thread.
 *   <li>The {@link CreateCollectionCmd} sees the state change in ZooKeeper and:
 *       <ol start="5">
 *         <li>Builds and sends requests to each node to create the appropriate cores for all the
 *             replicas of all shards of the collection. Nodes create the replicas and set them to
 *             {@link org.apache.solr.common.cloud.Replica.State#ACTIVE}.
 *       </ol>
 *   <li>The collection creation command has succeeded from the Overseer perspective,
 *   <li>{@link CollectionsHandler} checks the replicas in Zookeeper and verifies they are all
 *       {@link org.apache.solr.common.cloud.Replica.State#ACTIVE},
 *   <li>The client receives a success return.
 * </ol>
 */
public class Overseer implements SolrCloseable {
  public static final String QUEUE_OPERATION = "operation";

  // System properties are used in tests to make them run fast
  public static final int STATE_UPDATE_DELAY = ZkStateReader.STATE_UPDATE_DELAY;
  public static final int STATE_UPDATE_BATCH_SIZE =
      Integer.getInteger("solr.OverseerStateUpdateBatchSize", 10000);
  public static final int STATE_UPDATE_MAX_QUEUE =
      Integer.getInteger("solr.OverseerStateUpdateMaxQueueSize", 20000);

  public static final int NUM_RESPONSES_TO_STORE = 10000;
  public static final String OVERSEER_ELECT = "/overseer_elect";
  private final CopyOnWriteArrayList<Message> unprocessedMessages = new CopyOnWriteArrayList<>();

  private SolrMetricsContext solrMetricsContext;
  private volatile String metricTag = SolrMetricProducer.getUniqueMetricTag(this, null);

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  enum LeaderStatus {
    DONT_KNOW,
    NO,
    YES
  }

  /**
   * This class is responsible for dequeueing state change requests from the ZooKeeper queue at
   * <code>/overseer/queue</code> and executing the requested cluster change (essentially writing or
   * updating <code>state.json</code> for a collection).
   *
   * <p>The cluster state updater is a single thread dequeueing and executing requests.
   */
  private class ClusterStateUpdater implements Runnable, Closeable {

    private final ZkStateReader reader;
    private final SolrZkClient zkClient;
    private final String myId;
    // queue where everybody can throw tasks
    private final ZkDistributedQueue stateUpdateQueue;
    // TODO remove in 9.0, we do not push message into this queue anymore
    // Internal queue where overseer stores events that have not yet been published into cloudstate
    // If Overseer dies while extracting the main queue a new overseer will start from this queue
    private final ZkDistributedQueue workQueue;
    // Internal map which holds the information about running tasks.
    private final DistributedMap runningMap;
    // Internal map which holds the information about successfully completed tasks.
    private final DistributedMap completedMap;
    // Internal map which holds the information about failed tasks.
    private final DistributedMap failureMap;

    private final Stats zkStats;

    private SolrMetricsContext clusterStateUpdaterMetricContext;

    private final int minStateByteLenForCompression;

    private final Compressor compressor;

    private boolean isClosed = false;

    public ClusterStateUpdater(
        final ZkStateReader reader,
        final String myId,
        Stats zkStats,
        int minStateByteLenForCompression,
        Compressor compressor) {
      this.zkClient = reader.getZkClient();
      this.zkStats = zkStats;
      this.stateUpdateQueue = getStateUpdateQueue(zkStats);
      this.workQueue = getInternalWorkQueue(zkClient, zkStats);
      this.failureMap = getFailureMap(zkClient);
      this.runningMap = getRunningMap(zkClient);
      this.completedMap = getCompletedMap(zkClient);
      this.myId = myId;
      this.reader = reader;
      this.minStateByteLenForCompression = minStateByteLenForCompression;
      this.compressor = compressor;

      clusterStateUpdaterMetricContext = solrMetricsContext.getChildContext(this);
      clusterStateUpdaterMetricContext.gauge(
          () -> stateUpdateQueue.getZkStats().getQueueLength(),
          true,
          "stateUpdateQueueSize",
          "queue");
    }

    public Stats getStateUpdateQueueStats() {
      return stateUpdateQueue.getZkStats();
    }

    public Stats getWorkQueueStats() {
      return workQueue.getZkStats();
    }

    @Override
    public void run() {
      MDCLoggingContext.setNode(zkController.getNodeName());

      LeaderStatus isLeader = amILeader();
      while (isLeader == LeaderStatus.DONT_KNOW) {
        log.debug("am_i_leader unclear {}", isLeader);
        isLeader = amILeader(); // not a no, not a yes, try ask again
      }

      if (log.isInfoEnabled()) {
        log.info("Starting to work on the main queue : {}", LeaderElector.getNodeName(myId));
      }
      try {
        ZkStateWriter zkStateWriter = null;
        ClusterState clusterState = null;
        boolean refreshClusterState = true; // let's refresh in the first iteration
        // we write updates in batch, but if an exception is thrown when writing new clusterstate,
        // we do not sure which message is bad message, therefore we will re-process node one by one
        int fallbackQueueSize = Integer.MAX_VALUE;
        ZkDistributedQueue fallbackQueue = workQueue;
        while (!this.isClosed) {
          isLeader = amILeader();
          if (LeaderStatus.NO == isLeader) {
            break;
          } else if (LeaderStatus.YES != isLeader) {
            log.debug("am_i_leader unclear {}", isLeader);
            continue; // not a no, not a yes, try ask again
          }

          // TODO consider removing 'refreshClusterState' and simply check if clusterState is null
          if (refreshClusterState) {
            try {
              reader.forciblyRefreshAllClusterStateSlow();
              clusterState = reader.getClusterState();
              zkStateWriter =
                  new ZkStateWriter(reader, stats, minStateByteLenForCompression, compressor);
              refreshClusterState = false;

              // if there were any errors while processing
              // the state queue, items would have been left in the
              // work queue so let's process those first
              byte[] data = fallbackQueue.peek();
              while (fallbackQueueSize > 0 && data != null) {
                final ZkNodeProps message = ZkNodeProps.load(data);
                if (log.isDebugEnabled()) {
                  log.debug(
                      "processMessage: fallbackQueueSize: {}, message = {}",
                      fallbackQueue.getZkStats().getQueueLength(),
                      message);
                }
                try {
                  clusterState =
                      processQueueItem(message, clusterState, zkStateWriter, false, null);
                } catch (Exception e) {
                  if (isBadMessage(e)) {
                    log.warn(
                        "Exception when process message = {}, consider as bad message and poll out from the queue",
                        message);
                    fallbackQueue.poll();
                  }
                  throw e;
                }
                fallbackQueue.poll(); // poll-ing removes the element we got by peek-ing
                data = fallbackQueue.peek();
                fallbackQueueSize--;
              }
              // force flush at the end of the loop, if there are no pending updates, this is a no
              // op call
              clusterState = zkStateWriter.writePendingUpdates();
              // the workQueue is empty now, use stateUpdateQueue as fallback queue
              fallbackQueue = stateUpdateQueue;
              fallbackQueueSize = 0;
            } catch (IllegalStateException e) {
              return;
            } catch (KeeperException.SessionExpiredException e) {
              log.warn("Solr cannot talk to ZK, exiting Overseer work queue loop", e);
              return;
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              return;
            } catch (Exception e) {
              log.error("Exception in Overseer when process message from work queue, retrying", e);
              refreshClusterState = true;
              continue;
            }
          }

          ArrayDeque<Pair<String, byte[]>> queue = null;
          try {
            // We do not need to filter any nodes here cause all processed nodes are removed once we
            // flush clusterstate
            queue = new ArrayDeque<>(stateUpdateQueue.peekElements(1000, 3000L, (x) -> true));
          } catch (KeeperException.SessionExpiredException e) {
            log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (IllegalStateException e) {

          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
          }
          try {
            Set<String> processedNodes = new HashSet<>();
            while (queue != null && !queue.isEmpty()) {
              for (Pair<String, byte[]> head : queue) {
                byte[] data = head.second();
                final ZkNodeProps message = ZkNodeProps.load(data);
                if (log.isDebugEnabled()) {
                  log.debug(
                      "processMessage: queueSize: {}, message = {}",
                      stateUpdateQueue.getZkStats().getQueueLength(),
                      message);
                }

                processedNodes.add(head.first());
                fallbackQueueSize = processedNodes.size();
                // force flush to ZK after each message because there is no fallback if workQueue
                // items
                // are removed from workQueue but fail to be written to ZK
                while (unprocessedMessages.size() > 0) {
                  clusterState = zkStateWriter.writePendingUpdates();
                  Message m = unprocessedMessages.remove(0);
                  clusterState = m.run(clusterState, Overseer.this, zkStateWriter);
                }
                // The callback always be called on this thread
                clusterState =
                    processQueueItem(
                        message,
                        clusterState,
                        zkStateWriter,
                        true,
                        () -> {
                          stateUpdateQueue.remove(processedNodes);
                          processedNodes.clear();
                        });
              }
              if (isClosed) break;
              // if an event comes in the next 100ms batch it together
              queue =
                  new ArrayDeque<>(
                      stateUpdateQueue.peekElements(
                          1000, 100, node -> !processedNodes.contains(node)));
            }
            fallbackQueueSize = processedNodes.size();
            // we should force write all pending updates because the next iteration might sleep
            // until there are more items in the main queue
            clusterState = zkStateWriter.writePendingUpdates();
            // clean work queue
            stateUpdateQueue.remove(processedNodes);
            processedNodes.clear();
          } catch (KeeperException.SessionExpiredException e) {
            log.warn("Solr cannot talk to ZK, exiting Overseer main queue loop", e);
            return;
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          } catch (IllegalStateException e) {

          } catch (Exception e) {
            log.error("Exception in Overseer main queue loop", e);
            refreshClusterState = true; // it might have been a bad version error
          }
        }
      } finally {
        if (log.isInfoEnabled()) {
          log.info("Overseer Loop exiting : {}", LeaderElector.getNodeName(myId));
        }
        // do this in a separate thread because any wait is interrupted in this main thread
        Thread checkLeaderThread = new Thread(this::checkIfIamStillLeader, "OverseerExitThread");
        checkLeaderThread.setDaemon(true);
        checkLeaderThread.start();
      }
    }

    // Return true whenever the exception thrown by ZkStateWriter is correspond
    // to a invalid state or 'bad' message (in this case, we should remove that message from queue)
    private boolean isBadMessage(Exception e) {
      if (e instanceof KeeperException ke) {
        return ke.code() == KeeperException.Code.NONODE
            || ke.code() == KeeperException.Code.NODEEXISTS;
      }
      return !(e instanceof InterruptedException);
    }

    private ClusterState processQueueItem(
        ZkNodeProps message,
        ClusterState clusterState,
        ZkStateWriter zkStateWriter,
        boolean enableBatching,
        ZkStateWriter.ZkWriteCallback callback)
        throws Exception {
      final String operation = message.getStr(QUEUE_OPERATION);
      if (operation == null) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Message missing " + QUEUE_OPERATION + ":" + message);
      }
      List<ZkWriteCommand> zkWriteCommands = null;
      final Timer.Context timerContext = stats.time(operation);
      try {
        zkWriteCommands = processMessage(clusterState, message, operation);
        stats.success(operation);
      } catch (Exception e) {
        // generally there is nothing we can do - in most cases, we have
        // an issue that will fail again on retry or we cannot communicate with     a
        // ZooKeeper in which case another Overseer should take over
        // TODO: if ordering for the message is not important, we could
        // track retries and put it back on the end of the queue
        log.error(
            "Overseer could not process the current clusterstate state update message, skipping the message: {}",
            message,
            e);
        stats.error(operation);
      } finally {
        timerContext.stop();
      }
      if (zkWriteCommands != null) {
        clusterState = zkStateWriter.enqueueUpdate(clusterState, zkWriteCommands, callback);
        if (!enableBatching) {
          clusterState = zkStateWriter.writePendingUpdates();
        }
      }
      return clusterState;
    }

    private void checkIfIamStillLeader() {
      if (zkController != null
          && (zkController.getCoreContainer().isShutDown() || zkController.isClosed())) {
        return; // shutting down no need to go further
      }
      org.apache.zookeeper.data.Stat stat = new org.apache.zookeeper.data.Stat();
      final String path = OVERSEER_ELECT + "/leader";
      byte[] data;
      try {
        data = zkClient.getData(path, null, stat, true);
      } catch (IllegalStateException | KeeperException.NoNodeException e) {
        return;
      } catch (Exception e) {
        log.warn("Error communicating with ZooKeeper", e);
        return;
      }
      try {
        Map<?, ?> m = (Map<?, ?>) Utils.fromJSON(data);
        String id = (String) m.get(ID);
        if (overseerCollectionConfigSetProcessor.getId().equals(id)) {
          try {
            log.warn(
                "I (id={}) am exiting, but I'm still the leader",
                overseerCollectionConfigSetProcessor.getId());
            zkClient.delete(path, stat.getVersion(), true);
          } catch (KeeperException.BadVersionException e) {
            // no problem ignore it some other Overseer has already taken over
          } catch (Exception e) {
            log.error("Could not delete my leader node {}", path, e);
          }

        } else {
          log.info("somebody else (id={}) has already taken up the overseer position", id);
        }
      } finally {
        // if I am not shutting down, Then I need to rejoin election
        try {
          if (zkController != null && !zkController.getCoreContainer().isShutDown()) {
            zkController.rejoinOverseerElection(null, false);
          }
        } catch (Exception e) {
          log.warn("Unable to rejoinElection ", e);
        }
      }
    }

    private List<ZkWriteCommand> processMessage(
        ClusterState clusterState, final ZkNodeProps message, final String operation) {
      CollectionParams.CollectionAction collectionAction =
          CollectionParams.CollectionAction.get(operation);
      if (collectionAction != null) {
        switch (collectionAction) {
          case CREATE:
            return Collections.singletonList(
                new ClusterStateMutator(getSolrCloudManager())
                    .createCollection(clusterState, message));
          case DELETE:
            return Collections.singletonList(
                new ClusterStateMutator(getSolrCloudManager())
                    .deleteCollection(clusterState, message));
          case CREATESHARD:
            return Collections.singletonList(
                new CollectionMutator(getSolrCloudManager()).createShard(clusterState, message));
          case DELETESHARD:
            return Collections.singletonList(
                new CollectionMutator(getSolrCloudManager()).deleteShard(clusterState, message));
          case ADDREPLICA:
            return Collections.singletonList(
                new SliceMutator(getSolrCloudManager()).addReplica(clusterState, message));
          case ADDREPLICAPROP:
            return Collections.singletonList(
                new ReplicaMutator(getSolrCloudManager())
                    .addReplicaProperty(clusterState, message));
          case DELETEREPLICAPROP:
            return Collections.singletonList(
                new ReplicaMutator(getSolrCloudManager())
                    .deleteReplicaProperty(clusterState, message));
          case BALANCESHARDUNIQUE:
            ExclusiveSliceProperty dProp = new ExclusiveSliceProperty(clusterState, message);
            if (dProp.balanceProperty()) {
              String collName = message.getStr(ZkStateReader.COLLECTION_PROP);
              return Collections.singletonList(
                  new ZkWriteCommand(collName, dProp.getDocCollection()));
            }
            break;
          case MODIFYCOLLECTION:
            return Collections.singletonList(
                new CollectionMutator(getSolrCloudManager())
                    .modifyCollection(clusterState, message));
          default:
            throw new RuntimeException(
                "unknown operation:" + operation + " contents:" + message.getProperties());
        }
      } else {
        OverseerAction overseerAction = OverseerAction.get(operation);
        if (overseerAction == null) {
          throw new RuntimeException(
              "unknown operation:" + operation + " contents:" + message.getProperties());
        }
        switch (overseerAction) {
          case STATE:
            return Collections.singletonList(
                new ReplicaMutator(getSolrCloudManager()).setState(clusterState, message));
          case LEADER:
            return Collections.singletonList(
                new SliceMutator(getSolrCloudManager()).setShardLeader(clusterState, message));
          case DELETECORE:
            return Collections.singletonList(
                new SliceMutator(getSolrCloudManager()).removeReplica(clusterState, message));
          case ADDROUTINGRULE:
            return Collections.singletonList(
                new SliceMutator(getSolrCloudManager()).addRoutingRule(clusterState, message));
          case REMOVEROUTINGRULE:
            return Collections.singletonList(
                new SliceMutator(getSolrCloudManager()).removeRoutingRule(clusterState, message));
          case UPDATESHARDSTATE:
            return Collections.singletonList(
                new SliceMutator(getSolrCloudManager()).updateShardState(clusterState, message));
          case QUIT:
            if (myId.equals(message.get(ID))) {
              if (log.isInfoEnabled()) {
                log.info("Quit command received {} {}", message, LeaderElector.getNodeName(myId));
              }
              IOUtils.closeQuietly(overseerCollectionConfigSetProcessor);
              IOUtils.closeQuietly(this);
            } else {
              log.warn("Overseer received wrong QUIT message {}", message);
            }
            break;
          case DOWNNODE:
            return new NodeMutator(getSolrCloudManager()).downNode(clusterState, message);
          default:
            throw new RuntimeException(
                "unknown operation:" + operation + " contents:" + message.getProperties());
        }
      }

      return Collections.singletonList(ZkStateWriter.NO_OP);
    }

    private LeaderStatus amILeader() {
      Timer.Context timerContext = stats.time("am_i_leader");
      boolean success = true;
      String propsId = null;
      try {
        ZkNodeProps props =
            ZkNodeProps.load(zkClient.getData(OVERSEER_ELECT + "/leader", null, null, true));
        propsId = props.getStr(ID);
        if (myId.equals(propsId)) {
          return LeaderStatus.YES;
        }
      } catch (KeeperException e) {
        success = false;
        if (e.code() == KeeperException.Code.CONNECTIONLOSS) {
          log.error("", e);
          return LeaderStatus.DONT_KNOW;
        } else if (e.code() != KeeperException.Code.SESSIONEXPIRED) {
          log.warn("", e);
        } else {
          log.debug("", e);
        }
      } catch (InterruptedException e) {
        success = false;
        Thread.currentThread().interrupt();
      } catch (IllegalStateException e) {
        success = false;
      } catch (Exception e) {
        success = false;
        log.warn("Unexpected exception", e);
      } finally {
        timerContext.stop();
        if (success) {
          stats.success("am_i_leader");
        } else {
          stats.error("am_i_leader");
        }
      }
      log.info("According to ZK I (id={}) am no longer a leader. propsId={}", myId, propsId);
      return LeaderStatus.NO;
    }

    @Override
    public void close() {
      this.isClosed = true;
      clusterStateUpdaterMetricContext.unregister();
    }
  }

  public static class OverseerThread extends Thread implements Closeable {

    protected volatile boolean isClosed;
    private final Closeable thread;

    public <T extends Runnable & Closeable> OverseerThread(
        ThreadGroup ccTg, T thread, String name) {
      super(ccTg, thread, name);
      this.thread = thread;
    }

    @Override
    public void close() throws IOException {
      thread.close();
      this.isClosed = true;
    }

    public Closeable getThread() {
      return thread;
    }

    public boolean isClosed() {
      return this.isClosed;
    }
  }

  private OverseerThread ccThread;

  private OverseerThread updaterThread;

  private final ZkStateReader reader;

  private final HttpShardHandler shardHandler;

  private final UpdateShardHandler updateShardHandler;

  private final String adminPath;

  private OverseerCollectionConfigSetProcessor overseerCollectionConfigSetProcessor;

  private ZkController zkController;

  private Stats stats;
  private String id;
  private volatile boolean closed;
  private volatile boolean systemCollCompatCheck = true;

  private CloudConfig config;
  private final DistributedClusterStateUpdater distributedClusterStateUpdater;

  // overseer not responsible for closing reader
  public Overseer(
      HttpShardHandler shardHandler,
      UpdateShardHandler updateShardHandler,
      String adminPath,
      final ZkStateReader reader,
      ZkController zkController,
      CloudConfig config)
      throws KeeperException, InterruptedException {
    this.reader = reader;
    this.shardHandler = shardHandler;
    this.updateShardHandler = updateShardHandler;
    this.adminPath = adminPath;
    this.zkController = zkController;
    this.stats = new Stats();
    this.config = config;
    this.distributedClusterStateUpdater =
        new DistributedClusterStateUpdater(config.getDistributedClusterStateUpdates());

    this.solrMetricsContext =
        new SolrMetricsContext(
            zkController.getCoreContainer().getMetricManager(),
            SolrInfoBean.Group.overseer.toString(),
            metricTag);
  }

  public synchronized void start(String id) {
    MDCLoggingContext.setNode(zkController == null ? null : zkController.getNodeName());
    this.id = id;
    closed = false;
    doClose();
    stats = new Stats();
    log.info("Overseer (id={}) starting", id);
    createOverseerNode(reader.getZkClient());
    // launch cluster state updater thread
    ThreadGroup tg = new ThreadGroup("Overseer state updater.");
    String stateCompressionProviderClass = config.getStateCompressorClass();
    Compressor compressor =
        StrUtils.isNullOrEmpty(stateCompressionProviderClass)
            ? new ZLibCompressor()
            : zkController
                .getCoreContainer()
                .getResourceLoader()
                .newInstance(stateCompressionProviderClass, Compressor.class);
    updaterThread =
        new OverseerThread(
            tg,
            new ClusterStateUpdater(
                reader, id, stats, config.getMinStateByteLenForCompression(), compressor),
            "OverseerStateUpdate-" + id);
    updaterThread.setDaemon(true);

    ThreadGroup ccTg = new ThreadGroup("Overseer collection creation process.");

    // Below is the only non test usage of the "cluster state update" queue even when distributed
    // cluster state updates are enabled. That queue is used to tell the Overseer to quit. As long
    // as we have an Overseer, we need to support this.
    OverseerNodePrioritizer overseerPrioritizer =
        new OverseerNodePrioritizer(reader, this, adminPath, shardHandler.getShardHandlerFactory());
    overseerCollectionConfigSetProcessor =
        new OverseerCollectionConfigSetProcessor(
            reader,
            id,
            shardHandler,
            adminPath,
            stats,
            Overseer.this,
            overseerPrioritizer,
            solrMetricsContext);
    ccThread =
        new OverseerThread(
            ccTg,
            overseerCollectionConfigSetProcessor,
            "OverseerCollectionConfigSetProcessor-" + id);
    ccThread.setDaemon(true);

    updaterThread.start();
    ccThread.start();

    systemCollectionCompatCheck(
        new BiConsumer<>() {
          boolean firstPair = true;

          @Override
          public void accept(String s, Object o) {
            if (firstPair) {
              log.warn(
                  "WARNING: Collection '.system' may need re-indexing due to compatibility issues listed below. See REINDEXCOLLECTION documentation for more details.");
              firstPair = false;
            }
            log.warn("WARNING: *\t{}:\t{}", s, o);
          }
        });

    getCoreContainer().getClusterSingletons().startClusterSingletons();

    assert ObjectReleaseTracker.track(this);
  }

  public void systemCollectionCompatCheck(final BiConsumer<String, Object> consumer) {
    ClusterState clusterState = zkController.getClusterState();
    if (clusterState == null) {
      log.warn("Unable to check back-compat of .system collection - can't obtain ClusterState.");
      return;
    }
    DocCollection coll = clusterState.getCollectionOrNull(CollectionAdminParams.SYSTEM_COLL);
    if (coll == null) {
      return;
    }
    // check that all shard leaders are active
    boolean allActive = true;
    for (Slice s : coll.getActiveSlices()) {
      if (s.getLeader() == null || !s.getLeader().isActive(clusterState.getLiveNodes())) {
        allActive = false;
        break;
      }
    }
    if (allActive) {
      doCompatCheck(consumer);
    } else {
      // wait for all leaders to become active and then check
      zkController.zkStateReader.registerCollectionStateWatcher(
          CollectionAdminParams.SYSTEM_COLL,
          (liveNodes, state) -> {
            boolean active = true;
            if (state == null || liveNodes.isEmpty()) {
              return true;
            }
            for (Slice s : state.getActiveSlices()) {
              if (s.getLeader() == null || !s.getLeader().isActive(liveNodes)) {
                active = false;
                break;
              }
            }
            if (active) {
              doCompatCheck(consumer);
            }
            return active;
          });
    }
  }

  private void doCompatCheck(BiConsumer<String, Object> consumer) {
    if (systemCollCompatCheck) {
      systemCollCompatCheck = false;
    } else {
      return;
    }

    try (var solrClient =
            new Http2SolrClient.Builder()
                .withHttpClient(getCoreContainer().getDefaultHttpSolrClient())
                .withIdleTimeout(30000, TimeUnit.MILLISECONDS)
                .build();
        var client =
            new CloudHttp2SolrClient.Builder(
                    Collections.singletonList(getZkController().getZkServerAddress()),
                    Optional.empty())
                .withHttpClient(solrClient)
                .build()) {
      CollectionAdminRequest.ColStatus req =
          CollectionAdminRequest.collectionStatus(CollectionAdminParams.SYSTEM_COLL)
              .setWithSegments(true)
              .setWithFieldInfo(true);
      CollectionAdminResponse rsp = req.process(client);
      NamedList<?> status = (NamedList<?>) rsp.getResponse().get(CollectionAdminParams.SYSTEM_COLL);
      Collection<?> nonCompliant = (Collection<?>) status.get("schemaNonCompliant");
      if (!nonCompliant.contains("(NONE)")) {
        consumer.accept("indexFieldsNotMatchingSchema", nonCompliant);
      }
      Set<Integer> segmentCreatedMajorVersions = new HashSet<>();
      Set<String> segmentVersions = new HashSet<>();
      int currentMajorVersion = Version.LATEST.major;
      String currentVersion = Version.LATEST.toString();
      segmentVersions.add(currentVersion);
      segmentCreatedMajorVersions.add(currentMajorVersion);
      NamedList<?> shards = (NamedList<?>) status.get("shards");
      for (Map.Entry<String, ?> entry : shards) {
        NamedList<?> leader = (NamedList<?>) ((NamedList<?>) entry.getValue()).get("leader");
        if (leader == null) {
          continue;
        }
        NamedList<?> segInfos = (NamedList<?>) leader.get("segInfos");
        if (segInfos == null) {
          continue;
        }
        NamedList<?> infos = (NamedList<?>) segInfos.get("info");
        if (((Number) infos.get("numSegments")).intValue() > 0) {
          segmentVersions.add(infos.get("minSegmentLuceneVersion").toString());
        }
        if (infos.get("commitLuceneVersion") != null) {
          segmentVersions.add(infos.get("commitLuceneVersion").toString());
        }
        NamedList<?> segmentInfos = (NamedList<?>) segInfos.get("segments");
        segmentInfos.forEach(
            (k, v) -> {
              NamedList<?> segment = (NamedList<?>) v;
              segmentVersions.add(segment.get("version").toString());
              if (segment.get("minVersion") != null) {
                segmentVersions.add(segment.get("version").toString());
              }
              if (segment.get("createdVersionMajor") != null) {
                segmentCreatedMajorVersions.add(
                    ((Number) segment.get("createdVersionMajor")).intValue());
              }
            });
      }
      if (segmentVersions.size() > 1) {
        consumer.accept("differentSegmentVersions", segmentVersions);
        consumer.accept("currentLuceneVersion", currentVersion);
      }
      if (segmentCreatedMajorVersions.size() > 1) {
        consumer.accept("differentMajorSegmentVersions", segmentCreatedMajorVersions);
        consumer.accept("currentLuceneMajorVersion", currentMajorVersion);
      }

    } catch (SolrServerException | IOException e) {
      log.warn("Unable to perform back-compat check of .system collection", e);
    }
  }

  /** Start {@link ClusterSingleton} plugins when we become the leader. */

  /** Stop {@link ClusterSingleton} plugins when we lose leadership. */
  public Stats getStats() {
    return stats;
  }

  ZkController getZkController() {
    return zkController;
  }

  public CoreContainer getCoreContainer() {
    return zkController.getCoreContainer();
  }

  public SolrCloudManager getSolrCloudManager() {
    return zkController.getSolrCloudManager();
  }

  public DistributedClusterStateUpdater getDistributedClusterStateUpdater() {
    return distributedClusterStateUpdater;
  }

  /**
   * For tests.
   *
   * @lucene.internal
   * @return state updater thread
   */
  public synchronized OverseerThread getUpdaterThread() {
    return updaterThread;
  }

  @Override
  public synchronized void close() {
    if (this.id != null) {
      log.info("Overseer (id={}) closing", id);
    }
    // stop singletons only on the leader
    if (!this.closed) {
      getCoreContainer().getClusterSingletons().stopClusterSingletons();
    }
    this.closed = true;
    doClose();

    assert ObjectReleaseTracker.release(this);
  }

  @Override
  public boolean isClosed() {
    return closed;
  }

  private void doClose() {

    if (updaterThread != null) {
      IOUtils.closeQuietly(updaterThread);
      updaterThread.interrupt();
    }
    if (ccThread != null) {
      IOUtils.closeQuietly(ccThread);
      ccThread.interrupt();
    }
    if (updaterThread != null) {
      try {
        updaterThread.join();
      } catch (InterruptedException e) {
      }
    }
    if (ccThread != null) {
      try {
        ccThread.join();
      } catch (InterruptedException e) {
      }
    }
    updaterThread = null;
    ccThread = null;
  }

  /**
   * Get queue that can be used to send messages to Overseer.
   *
   * <p>Any and all modifications to the cluster state must be sent to the overseer via this queue.
   * The complete list of overseer actions supported by this queue are documented inside the {@link
   * OverseerAction} enum.
   *
   * <p>Performance statistics on the returned queue are <em>not</em> tracked by the Overseer Stats
   * API, see {@link
   * org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}. Therefore,
   * this method should be used only by clients for writing to the overseer queue.
   *
   * <p>This method will create the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @return a {@link ZkDistributedQueue} object
   */
  ZkDistributedQueue getStateUpdateQueue() {
    if (distributedClusterStateUpdater.isDistributedStateUpdate()) {
      throw new IllegalStateException(
          "Cluster state is done in a distributed way, should not try to access ZK queue");
    }
    return getStateUpdateQueue(new Stats());
  }

  /**
   * Separated into its own method from {@link #getStateUpdateQueue()} that does the same thing
   * because this one is legit to call even when cluster state updates are distributed whereas the
   * other one is not.
   */
  ZkDistributedQueue getOverseerQuitNotificationQueue() {
    return getStateUpdateQueue(new Stats());
  }

  /**
   * The overseer uses the returned queue to read any operations submitted by clients. This method
   * should not be used directly by anyone other than the Overseer itself. This method will create
   * the /overseer znode in ZooKeeper if it does not exist already.
   *
   * @param zkStats a {@link Stats} object which tracks statistics for all zookeeper operations
   *     performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  ZkDistributedQueue getStateUpdateQueue(Stats zkStats) {
    return new ZkDistributedQueue(
        reader.getZkClient(), "/overseer/queue", zkStats, STATE_UPDATE_MAX_QUEUE);
  }

  /**
   * Internal overseer work queue. This should not be used outside of Overseer.
   *
   * <p>This queue is used to store overseer operations that have been removed from the state update
   * queue but are being executed as part of a batch. Once the result of the batch is persisted to
   * zookeeper, these items are removed from the work queue. If the overseer dies while processing a
   * batch then a new overseer always operates from the work queue first and only then starts
   * processing operations from the state update queue. This method will create the /overseer znode
   * in ZooKeeper if it does not exist already.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @param zkStats a {@link Stats} object which tracks statistics for all zookeeper operations
   *     performed by this queue
   * @return a {@link ZkDistributedQueue} object
   */
  static ZkDistributedQueue getInternalWorkQueue(final SolrZkClient zkClient, Stats zkStats) {
    return new ZkDistributedQueue(zkClient, "/overseer/queue-work", zkStats);
  }

  /* Internal map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getRunningMap(final SolrZkClient zkClient) {
    return new DistributedMap(zkClient, "/overseer/collection-map-running");
  }

  /* Size-limited map for successfully completed tasks*/
  static DistributedMap getCompletedMap(final SolrZkClient zkClient) {
    return new SizeLimitedDistributedMap(
        zkClient,
        "/overseer/collection-map-completed",
        NUM_RESPONSES_TO_STORE,
        (child) -> getAsyncIdsMap(zkClient).remove(child));
  }

  /* Map for failed tasks, not to be used outside of the Overseer */
  static DistributedMap getFailureMap(final SolrZkClient zkClient) {
    return new SizeLimitedDistributedMap(
        zkClient,
        "/overseer/collection-map-failure",
        NUM_RESPONSES_TO_STORE,
        (child) -> getAsyncIdsMap(zkClient).remove(child));
  }

  /* Map of async IDs currently in use*/
  static DistributedMap getAsyncIdsMap(final SolrZkClient zkClient) {
    return new DistributedMap(zkClient, "/overseer/async_ids");
  }

  /**
   * Get queue that can be used to submit collection API tasks to the Overseer.
   *
   * <p>This queue is used internally by the {@link CollectionsHandler} to submit collection API
   * tasks which are executed by the {@link OverseerCollectionMessageHandler}. The actions supported
   * by this queue are listed in the {@link
   * org.apache.solr.common.params.CollectionParams.CollectionAction} enum.
   *
   * <p>Performance statistics on the returned queue are <em>not</em> tracked by the Overseer Stats
   * API, see {@link
   * org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient) {
    return getCollectionQueue(zkClient, new Stats());
  }

  /**
   * Get queue that can be used to read collection API tasks to the Overseer.
   *
   * <p>This queue is used internally by the {@link OverseerCollectionMessageHandler} to read
   * collection API tasks submitted by the {@link CollectionsHandler}. The actions supported by this
   * queue are listed in the {@link org.apache.solr.common.params.CollectionParams.CollectionAction}
   * enum.
   *
   * <p>Performance statistics on the returned queue are tracked by the Overseer Stats API, see
   * {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getCollectionQueue(final SolrZkClient zkClient, Stats zkStats) {
    return new OverseerTaskQueue(zkClient, "/overseer/collection-queue-work", zkStats);
  }

  /**
   * Get queue that can be used to submit configset API tasks to the Overseer.
   *
   * <p>This queue is used internally by the {@link org.apache.solr.handler.admin.ConfigSetsHandler}
   * to submit tasks which are executed by the {@link OverseerConfigSetMessageHandler}. The actions
   * supported by this queue are listed in the {@link
   * org.apache.solr.common.params.ConfigSetParams.ConfigSetAction} enum.
   *
   * <p>Performance statistics on the returned queue are <em>not</em> tracked by the Overseer Stats
   * API, see {@link
   * org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient) {
    return getConfigSetQueue(zkClient, new Stats());
  }

  /**
   * Get queue that can be used to read configset API tasks to the Overseer.
   *
   * <p>This queue is used internally by the {@link OverseerConfigSetMessageHandler} to read
   * configset API tasks submitted by the {@link org.apache.solr.handler.admin.ConfigSetsHandler}.
   * The actions supported by this queue are listed in the {@link
   * org.apache.solr.common.params.ConfigSetParams.ConfigSetAction} enum.
   *
   * <p>Performance statistics on the returned queue are tracked by the Overseer Stats API, see
   * {@link org.apache.solr.common.params.CollectionParams.CollectionAction#OVERSEERSTATUS}.
   *
   * <p>For now, this internally returns the same queue as {@link #getCollectionQueue(SolrZkClient,
   * Stats)}. It is the responsibility of the client to ensure that configset API actions are
   * prefixed with {@link OverseerConfigSetMessageHandler#CONFIGSETS_ACTION_PREFIX} so that it is
   * processed by {@link OverseerConfigSetMessageHandler}.
   *
   * @param zkClient the {@link SolrZkClient} to be used for reading/writing to the queue
   * @return a {@link ZkDistributedQueue} object
   */
  OverseerTaskQueue getConfigSetQueue(final SolrZkClient zkClient, Stats zkStats) {
    // For now, we use the same queue as the collection queue, but ensure
    // that the actions are prefixed with a unique string.
    return getCollectionQueue(zkClient, zkStats);
  }

  private void createOverseerNode(final SolrZkClient zkClient) {
    try {
      zkClient.create("/overseer", new byte[0], CreateMode.PERSISTENT, true);
    } catch (KeeperException.NodeExistsException e) {
      // ok
    } catch (InterruptedException e) {
      log.error("Could not create Overseer node", e);
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    } catch (KeeperException e) {
      log.error("Could not create Overseer node", e);
      throw new RuntimeException(e);
    }
  }

  public ZkStateReader getZkStateReader() {
    return reader;
  }

  public void offerStateUpdate(MapWriter mw) throws KeeperException, InterruptedException {
    offerStateUpdate(Utils.toJSON(mw));
  }

  public void offerStateUpdate(byte[] data) throws KeeperException, InterruptedException {
    // When cluster state update is distributed, the Overseer cluster state update queue should only
    // ever receive QUIT messages. These go to sendQuitToOverseer for execution path clarity.
    if (distributedClusterStateUpdater.isDistributedStateUpdate()) {
      final ZkNodeProps message = ZkNodeProps.load(data);
      final String operation = message.getStr(QUEUE_OPERATION);
      log.error(
          "Received unexpected message on Overseer cluster state updater for "
              + operation
              + " when distributed updates are configured"); // nowarn
      throw new RuntimeException(
          "Message "
              + operation
              + " offered to state update queue when distributed state update is configured.");
    }
    if (zkController.getZkClient().isClosed()) {
      throw new AlreadyClosedException();
    }
    getStateUpdateQueue().offer(data);
  }

  /**
   * Submit an intra-process message which will be picked up and executed when {@link
   * ClusterStateUpdater}'s loop runs next time
   */
  public void submit(Message message) {
    unprocessedMessages.add(message);
  }

  public interface Message {
    ClusterState run(ClusterState clusterState, Overseer overseer, ZkStateWriter zksw)
        throws Exception;
  }

  /**
   * This method enqueues a QUIT message to the overseer of given id. Effect is similar to building
   * the message then calling {@link #offerStateUpdate} but this method can legitimately be called
   * when cluster state update is distributed (and Overseer cluster state updater not really used)
   * while {@link #offerStateUpdate} is not. Note that sending "QUIT" to overseer is not a cluster
   * state update and was likely added to this queue because it was simpler.
   */
  public void sendQuitToOverseer(String overseerId) throws KeeperException, InterruptedException {
    getOverseerQuitNotificationQueue()
        .offer(
            ew ->
                ew.put(Overseer.QUEUE_OPERATION, OverseerAction.QUIT.toLower())
                    .put(ID, overseerId));
  }
}
