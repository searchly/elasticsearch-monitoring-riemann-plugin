package com.searchly.riemann.service;

import com.aphyr.riemann.client.EventDSL;
import com.aphyr.riemann.client.RiemannClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.NodeIndicesStats;
import org.elasticsearch.monitor.MonitorService;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.jvm.JvmStats;
import org.elasticsearch.monitor.os.OsStats;

import java.util.HashMap;
import java.util.List;
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
    private List<String> tags;
    private Map<String, String> attributes;

    public static NodeStatsRiemannEvent getNodeStatsRiemannEvent(RiemannClient riemannClient,
                                                                 Settings settings, String hostDefinition, String clusterName, List<String> tags, Map<String, String> attributes) {
        if (nodeStatsRiemannEvent == null) {
            nodeStatsRiemannEvent = new NodeStatsRiemannEvent(riemannClient, settings, hostDefinition, clusterName, tags, attributes);
        }
        return nodeStatsRiemannEvent;
    }

    private NodeStatsRiemannEvent(RiemannClient riemannClient, Settings settings, String hostDefinition, String clusterName, List<String> tags, Map<String, String> attributes) {
        this.riemannClient = riemannClient;
        this.hostDefinition = hostDefinition;
        this.settings = settings;
        this.tags = tags;
        this.attributes = attributes;
        this.deltaMap = new HashMap<String, Long>();
        // init required delta instead of null check
        deltaMap.put("index_rate", 0L);
        deltaMap.put("query_rate", 0L);
        deltaMap.put("fetch_rate", 0L);

    }

    public void sendEvents(MonitorService monitorService, NodeIndicesStats nodeIndicesStats) {

        //JVM
        //mem
        if (settings.getAsBoolean("metrics.riemann.heap_ratio.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.heap_ratio.ok", 85L);
            long warning = settings.getAsLong("metrics.riemann.heap_ratio.warning", 95L);
            heapRatio(monitorService.jvmService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.current_query_rate.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.current_query_rate.ok", 50L);
            long warning = settings.getAsLong("metrics.riemann.current_query_rate.warning", 70L);
            currentQueryRate(nodeIndicesStats, ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.current_fetch_rate.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.current_fetch_rate.ok", 50L);
            long warning = settings.getAsLong("metrics.riemann.current_fetch_rate.warning", 70L);
            currentFetchRate(nodeIndicesStats, ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.current_indexing_rate.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.current_indexing_rate.ok", 300L);
            long warning = settings.getAsLong("metrics.riemann.current_indexing_rate.warning", 1000L);
            currentIndexingRate(nodeIndicesStats, ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.total_thread_count.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.total_thread_count.ok", 150L);
            long warning = settings.getAsLong("metrics.riemann.total_thread_count.warning", 200L);
            totalThreadCount(monitorService.jvmService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.system_load.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.system_load.ok", 2L);
            long warning = settings.getAsLong("metrics.riemann.system_load.warning", 5L);
            systemLoad(monitorService.osService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.system_memory_usage.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.system_memory_usage.ok", 80L);
            long warning = settings.getAsLong("metrics.riemann.system_memory_usage.warning", 90L);
            systemMemory(monitorService.osService().stats(), ok, warning);
        }

        if (settings.getAsBoolean("metrics.riemann.disk_usage.enabled", true)) {
            long ok = settings.getAsLong("metrics.riemann.disk_usage.ok", 80L);
            long warning = settings.getAsLong("metrics.riemann.disk_usage.warning", 90L);
            systemFile(monitorService.fsService().stats(), ok, warning);
        }

    }

    private EventDSL buildEvent() {
        return riemannClient.event().host(hostDefinition).tags(tags).attributes(attributes);
    }

    private void currentIndexingRate(NodeIndicesStats nodeIndicesStats, long ok, long warning) {
        long indexCount = nodeIndicesStats.getIndexing().getTotal().getIndexCount();
        long delta = deltaMap.get("index_rate");
        long indexingCurrent = indexCount - delta;
        deltaMap.put("index_rate", indexCount);
        buildEvent().service("Current Indexing Rate").description("current_indexing_rate").state(RiemannUtils.getState(indexingCurrent, ok, warning)).metric(indexingCurrent).send();
    }

    private void heapRatio(JvmStats jvmStats, long ok, long warning) {
        long heapUsed = jvmStats.getMem().getHeapUsed().getBytes();
        long heapCommitted = jvmStats.getMem().getHeapCommitted().getBytes();
        long heapRatio = (heapUsed * 100) / heapCommitted;
        buildEvent().service("Heap Usage Ratio %").description("heap_usage_ratio").state(RiemannUtils.getState(heapRatio, ok, warning)).metric(heapRatio).send();
    }

    private void currentQueryRate(NodeIndicesStats nodeIndicesStats, long ok, long warning) {
        long queryCount = nodeIndicesStats.getSearch().getTotal().getQueryCount();

        long delta = deltaMap.get("query_rate");
        long queryCurrent = queryCount - delta;
        deltaMap.put("query_rate", queryCount);

        buildEvent().service("Current Query Rate").description("current_query_rate").state(RiemannUtils.getState(queryCurrent, ok, warning)).metric(queryCurrent).send();
    }

    private void currentFetchRate(NodeIndicesStats nodeIndicesStats, long ok, long warning) {
        long fetchCount = nodeIndicesStats.getSearch().getTotal().getFetchCount();
        long delta = deltaMap.get("fetch_rate");
        long fetchCurrent = fetchCount - delta;
        deltaMap.put("fetch_rate", fetchCount);
        buildEvent().service("Current Fetch Rate").description("current_fetch_rate").state(RiemannUtils.getState(fetchCurrent, ok, warning)).metric(fetchCurrent).send();
    }

    private void totalThreadCount(JvmStats jvmStats, long ok, long warning) {
        int threadCount = jvmStats.getThreads().getCount();
        buildEvent().service("Total Thread Count").description("total_thread_count").state(RiemannUtils.getState(threadCount, ok, warning)).metric(threadCount).send();
    }

    private void systemLoad(OsStats osStats, long ok, long warning) {
        double[] systemLoad = osStats.getCpu().getLoadAverage();
        riemannClient.event().host(hostDefinition).
                service("System Load").description("system_load").tags(tags).attributes(attributes).state(RiemannUtils.getState((long) systemLoad[0], ok, warning)).metric(systemLoad[0]).send();
    }

    private void systemMemory(OsStats osStats, long ok, long warning) {
        short memoryUsedPercentage = osStats.getMem().getUsedPercent();
        buildEvent().service("System Memory Usage %").description("system_memory_usage").state(RiemannUtils.getState(memoryUsedPercentage, ok, warning)).metric(memoryUsedPercentage).send();

    }

    private void systemFile(FsInfo fsInfo, long ok, long warning) {
        long free = fsInfo.getTotal().getFree().getBytes();
        long total = fsInfo.getTotal().getTotal().getBytes();
        long usageRatio = ((total - free) * 100) / total;
        buildEvent().service("Disk Usage %").description("system_disk_usage").state(RiemannUtils.getState(usageRatio, ok, warning)).metric(usageRatio).send();
    }
}
