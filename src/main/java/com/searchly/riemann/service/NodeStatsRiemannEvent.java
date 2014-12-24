package com.searchly.riemann.service;

import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsStats;
import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.monitor.process.ProcessService;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ferhat
 */
public class NodeStatsRiemannEvent {

    private RiemannClient riemannClient;
    private String hostDefinition;
    private Settings settings;
    private static NodeStatsRiemannEvent nodeStatsRiemannEvent;
    private Map<String, Long> deltaMap;
    private String[] tags;

    public static NodeStatsRiemannEvent getNodeStatsRiemannEvent(RiemannClient riemannClient,
                                                                 Settings settings, String hostDefinition, String clusterName, String[] tags) {
        if (nodeStatsRiemannEvent == null) {
            nodeStatsRiemannEvent = new NodeStatsRiemannEvent(riemannClient, settings, hostDefinition, clusterName, tags);
        }
        return nodeStatsRiemannEvent;
    }

    private NodeStatsRiemannEvent(RiemannClient riemannClient, Settings settings, String hostDefinition, String clusterName, String[] tags) {
        this.riemannClient = riemannClient;
        this.hostDefinition = hostDefinition;
        this.settings = settings;
        this.tags = tags;
        this.deltaMap = new HashMap<String, Long>();
        // init required delta instead of null check
        deltaMap.put("index_rate", 0L);
        deltaMap.put("query_rate", 0L);
        deltaMap.put("fetch_rate", 0L);

    }

    public void sendEvents(MonitorService monitorService, NodeIndicesStats nodeIndicesStats) {

        //JVM
        //mem
        if (settings.getAsBoolean("metrics.riemann.heap_ratio", true)) {
            heapRatio(monitorService.jvmService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.current_query_rate", true)) {
            currentQueryRate(nodeIndicesStats);
        }

        if (settings.getAsBoolean("metrics.riemann.current_fetch_rate", true)) {
            currentFetchRate(nodeIndicesStats);
        }

        if (settings.getAsBoolean("metrics.riemann.current_indexing_rate", true)) {
            currentIndexingRate(nodeIndicesStats);
        }

        if (settings.getAsBoolean("metrics.riemann.total_thread_count", true)) {
            totalThreadCount(monitorService.jvmService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.system_load", true)) {
            systemLoadOne(monitorService.osService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.system_load_5m", true)) {
            systemLoadFive(monitorService.osService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.system_load_15m", true)) {
            systemLoadFifteen(monitorService.osService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.system_memory_usage", true)) {
            systemMemory(monitorService.osService().stats());
        }

        if (settings.getAsBoolean("metrics.riemann.disk_usage", true)) {
            systemFile(monitorService.fsService().stats());
        }

    }

    private void currentIndexingRate(NodeIndicesStats nodeIndicesStats) {
        long indexCount = nodeIndicesStats.getIndexing().getTotal().getIndexCount();
        long delta = deltaMap.get("index_rate");
        long indexingCurrent = indexCount - delta;
        deltaMap.put("index_rate", indexCount);
        riemannClient.event().host(hostDefinition).
                service("Current Indexing Rate").description("current_indexing_rate").tags(tags).state(RiemannUtils.getState(indexingCurrent, 300, 1000)).metric(indexingCurrent).send();
    }

    private void heapRatio(JvmStats jvmStats) {
        long heapUsed = jvmStats.getMem().getHeapUsed().getBytes();
        long heapCommitted = jvmStats.getMem().getHeapCommitted().getBytes();
        long heapRatio = (heapUsed * 100) / heapCommitted;
        riemannClient.event().host(hostDefinition).
                service("Heap Usage Ratio %").description("heap_usage_ratio").tags(tags).state(RiemannUtils.getState(heapRatio, 85, 95)).metric(heapRatio).send();
    }

    private void currentQueryRate(NodeIndicesStats nodeIndicesStats) {
        long queryCount = nodeIndicesStats.getSearch().getTotal().getQueryCount();

        long delta = deltaMap.get("query_rate");
        long queryCurrent = queryCount - delta;
        deltaMap.put("query_rate", queryCount);

        riemannClient.event().host(hostDefinition).
                service("Current Query Rate").description("current_query_rate").tags(tags).state(RiemannUtils.getState(queryCurrent, 50, 70)).metric(queryCurrent).send();
    }

    private void currentFetchRate(NodeIndicesStats nodeIndicesStats) {
        long fetchCount = nodeIndicesStats.getSearch().getTotal().getFetchCount();
        long delta = deltaMap.get("fetch_rate");
        long fetchCurrent = fetchCount - delta;
        deltaMap.put("fetch_rate", fetchCount);
        riemannClient.event().host(hostDefinition).
                service("Current Fetch Rate").description("current_fetch_rate").tags(tags).state(RiemannUtils.getState(fetchCurrent, 50, 70)).metric(fetchCurrent).send();
    }

    private void totalThreadCount(JvmStats jvmStats) {
        int threadCount = jvmStats.getThreads().getCount();
        riemannClient.event().host(hostDefinition).
                service("Total Thread Count").description("total_thread_count").tags(tags).state(RiemannUtils.getState(threadCount, 150, 200)).metric(threadCount).send();
    }

    private void systemLoadOne(OsStats osStats) {
        double[] systemLoad = osStats.getLoadAverage();
        riemannClient.event().host(hostDefinition).
                service("System Load(1m)").description("system_load").tags(tags).state(RiemannUtils.getState((long) systemLoad[0], 2, 5)).metric(systemLoad[0]).send();
    }

    private void systemLoadFive(OsStats osStats) {
        double[] systemLoad = osStats.getLoadAverage();
        riemannClient.event().host(hostDefinition).
                service("System Load(5m)").description("system_load").tags(tags).state(RiemannUtils.getState((long) systemLoad[1], 2, 5)).metric(systemLoad[1]).send();
    }

    private void systemLoadFifteen(OsStats osStats) {
        double[] systemLoad = osStats.getLoadAverage();
        riemannClient.event().host(hostDefinition).
                service("System Load(15m)").description("system_load").tags(tags).state(RiemannUtils.getState((long) systemLoad[2], 2, 5)).metric(systemLoad[2]).send();
    }

    private void systemMemory(OsStats osStats) {
        short memoryUsedPercentage = osStats.getMem().getUsedPercent();
        riemannClient.event().host(hostDefinition).
                service("System Memory Usage %").description("system_memory_usage").tags(tags).state(RiemannUtils.getState(memoryUsedPercentage, 80, 90)).metric(memoryUsedPercentage).send();

    }

    private void systemFile(FsStats fsStats) {
        for (FsStats.Info info : fsStats) {
            long free = info.getFree().getBytes();
            long total = info.getTotal().getBytes();
            long usageRatio = ((total - free) * 100) / total;
            riemannClient.event().host(hostDefinition).
                    service("Disk Usage %").description("system_disk_usage").tags(tags).state(RiemannUtils.getState(usageRatio, 80, 90)).metric(usageRatio).send();
        }
    }
}
