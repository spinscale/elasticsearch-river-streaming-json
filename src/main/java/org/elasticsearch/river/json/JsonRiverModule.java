package org.elasticsearch.river.json;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.river.River;

public class JsonRiverModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(River.class).to(JsonRiver.class).asEagerSingleton();
    }
}
