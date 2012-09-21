package org.elasticsearch.plugin.river.json;

import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.river.RiversModule;
import org.elasticsearch.river.json.JsonRiverModule;

public class JsonRiverPlugin extends AbstractPlugin {

    @Override
    public String name() {
        return "json";
    }

    @Override
    public String description() {
        return "River Streaming JSON Plugin";
    }

    public void onModule(RiversModule module) {
        module.registerRiver("json", JsonRiverModule.class);
    }
}
