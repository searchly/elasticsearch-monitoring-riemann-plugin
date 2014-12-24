package com.searchly.riemann.service;

import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.state.TransportClusterStateAction;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.master.info.TransportClusterInfoAction;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.monitor.MonitorService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Timer;
import java.util.TimerTask;

public class RiemannService extends AbstractLifecycleComponent<RiemannService> {

    private final ClusterService clusterService;
    private final String riemannHost;
    private final Integer riemannPort;
    private final TimeValue riemannRefreshInternal;
    private MonitorService monitorService;
    private IndicesService indicesService;

    private final String clusterName;
    private RiemannClient riemannClient;
    private final TransportClusterHealthAction transportClusterHealthAction;
    private String[] tags;

    Timer timer = new Timer();

    @Inject
    public RiemannService(Settings settings,
                          ClusterService clusterService,
                          TransportClusterHealthAction transportClusterHealthAction,
                          MonitorService monitorService,
                          IndicesService indicesService) {
        super(settings);
        this.clusterService = clusterService;
        riemannRefreshInternal = settings.getAsTime("metrics.riemann.every", TimeValue.timeValueSeconds(1));
        riemannHost = settings.get("metrics.riemann.host", "localhost");
        riemannPort = settings.getAsInt("metrics.riemann.port", 5555);
        clusterName = settings.get("cluster.name");
        tags = settings.getAsArray("metrics.riemann.tags", new String[]{clusterName});
        try {
            riemannClient = RiemannClient.udp(new InetSocketAddress(riemannHost, riemannPort));
        } catch (IOException e) {
            logger.error("Can not create Riemann UDP connection", e);
        }
        this.transportClusterHealthAction = transportClusterHealthAction;
        this.monitorService = monitorService;
        this.indicesService = indicesService;
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        try {
            riemannClient.connect();
        } catch (IOException e) {
            logger.error("Can not connect to Riemann", e);
        }
        if (riemannHost != null && riemannHost.length() > 0) {

            timer.scheduleAtFixedRate(new RiemannTask(), riemannRefreshInternal.millis(), riemannRefreshInternal.millis());

            logger.info("Riemann reporting triggered every [{}] to host [{}:{}]", riemannRefreshInternal, riemannHost, riemannPort);
        } else {
            logger.error("Riemann reporting disabled, no riemann host configured");
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
        try {
            riemannClient.disconnect();
        } catch (IOException e) {
            logger.error("Riemann connection can not be closed", e);
        }
        logger.info("Riemann reporter stopped");
    }

    @Override
    protected void doClose() throws ElasticsearchException {
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

                    final String hostDefinition = clusterName + ":" + node.name();

                    if (settings.getAsBoolean("metrics.riemann.health", true)) {

                        transportClusterHealthAction.execute(new ClusterHealthRequest(), new ActionListener<ClusterHealthResponse>() {
                            @Override
                            public void onResponse(ClusterHealthResponse clusterIndexHealths) {
                                riemannClient.event().host(hostDefinition).service("Cluster Health").description("cluster_health").tags(tags)
                                        .state(RiemannUtils.getStateWithClusterInformation(clusterIndexHealths.getStatus().name())).send();
                            }

                            @Override
                            public void onFailure(Throwable throwable) {
                                riemannClient.event().host(hostDefinition).service("Cluster Health").description("cluster_health").tags(tags).state("critical").send();
                            }
                        });
                    }

                    NodeStatsRiemannEvent nodeStatsRiemannEvent = NodeStatsRiemannEvent.getNodeStatsRiemannEvent(riemannClient, settings, hostDefinition, clusterName, tags);
                    nodeStatsRiemannEvent.sendEvents(monitorService, indicesService.stats(true, new CommonStatsFlags(CommonStatsFlags.Flag.Docs, CommonStatsFlags.Flag.Store, CommonStatsFlags.Flag.Indexing, CommonStatsFlags.Flag.Get, CommonStatsFlags.Flag.Search)));


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
