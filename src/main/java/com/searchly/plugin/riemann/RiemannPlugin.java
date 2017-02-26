package com.searchly.plugin.riemann;

import com.searchly.riemann.service.RiemannService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.util.Collections.emptyList;

public class RiemannPlugin extends Plugin {

    private final Settings settings;

    public RiemannPlugin(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Class<? extends LifecycleComponent>> getGuiceServiceClasses() {
        return Collections.singletonList(RiemannService.class);
    }

    @Override
    public List<Setting<?>> getSettings() {
        Setting<List<String>> METRICS_RIEMANN_INTERVAL = Setting.listSetting("metrics.riemann.every", emptyList(), Function.identity(), Setting.Property.NodeScope);
        Setting<List<String>> METRICS_RIEMANN_HOST = Setting.listSetting("metrics.riemann.host", emptyList(), Function.identity(), Setting.Property.NodeScope);
        Setting<List<String>> METRICS_RIEMANN_TAGS = Setting.listSetting("metrics.riemann.tags", emptyList(), Function.identity(), Setting.Property.NodeScope);

        return Arrays.asList(METRICS_RIEMANN_INTERVAL, METRICS_RIEMANN_HOST, METRICS_RIEMANN_TAGS);
    }

}
