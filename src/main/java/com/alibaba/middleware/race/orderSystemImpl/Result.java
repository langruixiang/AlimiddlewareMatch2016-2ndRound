package com.alibaba.middleware.race.orderSystemImpl;

import com.alibaba.middleware.race.orderSystemInterface.OrderSystem;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by jiangchao on 2016/7/13.
 */
public class Result implements OrderSystem.Result{

    Long orderid;

    Map<String, KeyValue> keyValues = new HashMap<String, KeyValue>();

    public Long getOrderid() {
        return orderid;
    }

    public void setOrderid(Long orderid) {
        this.orderid = orderid;
    }

    public Map<String, KeyValue> getKeyValues() {
        return keyValues;
    }

    public void setKeyValues(Map<String, KeyValue> keyValues) {
        this.keyValues = keyValues;
    }

    @Override
    public KeyValue get(String key) {
        return keyValues.get(key);
    }

    @Override
    public KeyValue[] getAll() {
        List<KeyValue> keyValueList = (List<KeyValue>) keyValues.values();
        return (KeyValue[]) keyValueList.toArray();
    }

    @Override
    public long orderId() {
        return Long.valueOf(orderId());
    }

    @Override public String toString() {
        return "Result{" +
               "orderid=" + orderid +
               ", keyValues=" + keyValues +
               '}';
    }
}
