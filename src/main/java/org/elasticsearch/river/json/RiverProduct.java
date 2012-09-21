package org.elasticsearch.river.json;

import java.util.Map;

public class RiverProduct {

    public Action action;
    public String id;
    public Map<String, Object> product;

    public static enum Action {
        INDEX, DELETE
    }

    public static RiverProduct delete(String id) {
        RiverProduct product = new RiverProduct();
        product.action = Action.DELETE;
        product.id = id;
        return product;
    }

    public static RiverProduct index(String id, Map<String, Object> product) {
        RiverProduct riverProduct = new RiverProduct();
        riverProduct.action = Action.INDEX;
        riverProduct.id = id;
        riverProduct.product = product;
        return riverProduct;
    }

}
