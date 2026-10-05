package com.searchly.riemann.service;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.SpecialPermission;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.node.NodeService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;

/**
 * TODO: change service name to match general Riemann approach - should include more details
 * TODO: cluster health status for single node clusters
 * TODO: Expand metrics recorded? http://radar.oreilly.com/2015/04/10-elasticsearch-metrics-to-watch.html
 */
public class RiemannService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(RiemannService.class);

    private final ClusterService clusterService;
    private final String riemannHost;
    private final Integer riemannPort;
    private final TimeValue riemannRefreshInternal;
    private MonitorService monitorService;
    private IndicesService indicesService;

    private final String clusterName;
    private RiemannClient riemannClient;
    private List<String> tags;
    private Map<String, String> attributes = new HashMap<>();

    private Timer timer = new Timer();

    private Settings settings;

    /*
     * Cluster health is a function of the cluster state only: it is computed from this node's copy of the cluster state,
     * and only again when that state changes. Asking the master every interval made every node trigger a walk over all
     * shards of the cluster on the master, every second by default.
     */
    private String healthStateUUID;
    private String healthState;

    /*
     * Only the indexing and search counters are reported. Docs and store stats (which open every shard's searcher and
     * list its files) and get stats were computed every interval and thrown away.
     */
    private static final CommonStatsFlags REPORTED_INDICES_STATS =
            new CommonStatsFlags(CommonStatsFlags.Flag.Indexing, CommonStatsFlags.Flag.Search);

    @Inject
    public RiemannService(Settings settings,
                          ClusterService clusterService,
                          NodeService nodeService,
                          IndicesService indicesService) {
        super();
        this.settings = settings;
        this.clusterService = clusterService;
        riemannRefreshInternal = settings.getAsTime("metrics.riemann.every", TimeValue.timeValueSeconds(1));
        riemannHost = settings.get("metrics.riemann.host", "");
        riemannPort = settings.getAsInt("metrics.riemann.port", 5555);
        clusterName = settings.get("cluster.name");
        tags = settings.getAsList("metrics.riemann.tags", Collections.singletonList(clusterName));
        this.monitorService = nodeService.getMonitorService();
        this.indicesService = indicesService;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (riemannHost != null && riemannHost.length() > 0) {
            try {
                riemannClient = RiemannClient.udp(new InetSocketAddress(riemannHost, riemannPort));
                SecurityManager sm = System.getSecurityManager();

                if (sm != null) {
                    sm.checkPermission(new SpecialPermission());
                }

                AccessController.doPrivileged((PrivilegedAction<Object>) () -> {
                    try {
                        riemannClient.connect();
                    } catch (IOException e) {
                        logger.error(e);
                    }
                    return null;
                });

                timer.scheduleAtFixedRate(new RiemannTask(), riemannRefreshInternal.millis(), riemannRefreshInternal.millis());

                logger.info("Riemann reporting triggered every [{}] to host [{}:{}]", riemannRefreshInternal, riemannHost, riemannPort);
            } catch (IOException e) {
                logger.error("Can not connect to Riemann", e);
            }
        } else {
            logger.warn("Riemann reporting disabled, no riemann host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        try {
            riemannClient.close();
        } catch (RuntimeException e) {
            logger.error("Riemann connection can not be closed", e);
        }
        logger.info("Riemann reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    /** "ok", "warning" or "critical" for green, yellow or red, and "critical" without an elected master. */
    private String clusterHealthState() {
        ClusterState state = clusterService.state();
        if (state.nodes().getMasterNodeId() == null) {
            // the health request to the master used to fail in that case
            return "critical";
        }
        if (state.stateUUID().equals(healthStateUUID) == false) {
            healthState = RiemannUtils.getStateWithClusterInformation(new ClusterStateHealth(state).getStatus().name());
            healthStateUUID = state.stateUUID();
        }
        return healthState;
    }

    class RiemannTask extends TimerTask {

        @Override
        public void run() {
            logger.debug("running  RiemannTask");
            if (riemannClient.isConnected()) {
                logger.debug("getting data via discovery node...");
                DiscoveryNode node = clusterService.localNode();
                boolean isClusterStarted = clusterService.lifecycleState().equals(Lifecycle.State.STARTED);
                if (isClusterStarted && node != null) {

                    final String hostDefinition = clusterName + ":" + node.getName();

                    if (settings.getAsBoolean("metrics.riemann.health", true)) {
                        riemannClient.event().host(hostDefinition).service("Cluster Health").description("cluster_health")
                                .tags(tags).attributes(attributes).state(clusterHealthState()).send();
                    }

                    NodeStatsRiemannEvent nodeStatsRiemannEvent = NodeStatsRiemannEvent.getNodeStatsRiemannEvent(riemannClient, settings, hostDefinition, clusterName, tags, attributes);
                    nodeStatsRiemannEvent.sendEvents(monitorService, indicesService.stats(REPORTED_INDICES_STATS));


                    logger.debug("event sent to riemann");

                } else {
                    if (node != null) {
                        logger.info("[{}]/[{}] is not started", node.getId(), node.getName());
                    } else {
                        logger.info("Node is null!");
                    }

                }
            }
        }
    }
}
