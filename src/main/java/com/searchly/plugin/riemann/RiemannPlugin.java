package com.searchly.plugin.riemann;

import com.searchly.riemann.service.RiemannService;
import org.elasticsearch.common.collect.Lists;
import org.elasticsearch.common.component.LifecycleComponent;
import org.elasticsearch.plugins.AbstractPlugin;

import java.util.Collection;

public class RiemannPlugin extends AbstractPlugin {

    public String name() {
        return "riemann";
    }

    public String description() {
        return "Riemann Monitoring Plugin";
    }

    @SuppressWarnings("rawtypes")
    @Override public Collection<Class<? extends LifecycleComponent>> services() {
        Collection<Class<? extends LifecycleComponent>> services = Lists.newArrayList();
        services.add(RiemannService.class);
        return services;
    }

}
