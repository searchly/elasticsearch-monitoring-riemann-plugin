package com.searchly.plugin.riemann;

import com.searchly.riemann.service.RiemannService;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;

public class RiemannPlugin extends Plugin {

    private final Settings settings;

    public RiemannPlugin(Settings settings) {
        this.settings = settings;
    }

    public String name() {
        return "riemann";
    }

    public String description() {
        return "Riemann Monitoring Plugin";
    }

    public Collection<Class<? extends LifecycleComponent>> nodeServices() {
        Collection<Class<? extends LifecycleComponent>> services = new ArrayList<>();
        services.add(RiemannService.class);
        return services;
    }

}
