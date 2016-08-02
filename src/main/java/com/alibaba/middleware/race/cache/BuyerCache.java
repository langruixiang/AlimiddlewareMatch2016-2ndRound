package com.alibaba.middleware.race.cache;

import com.alibaba.middleware.race.model.Buyer;
import com.alibaba.middleware.race.orderSystemImpl.KeyValue;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Created by jiangchao on 2016/8/1.
 */
public class BuyerCache extends Thread{
    public static Map<String, Buyer> buyerMap = new ConcurrentHashMap<String, Buyer>();
    private Collection<String> buyerFiles;
    private CountDownLatch countDownLatch;
//    public static ChronicleMap<String, Buyer> buyerChronicleMap;
//
//    public static ChronicleMap createReplicatedMap(){
//        TcpTransportAndNetworkConfig tcpTransportAndNetworkConfig=TcpTransportAndNetworkConfig.of(8076).heartBeatInterval(1L, TimeUnit.SECONDS);
//        try {
//            ChronicleMapBuilder builder=ChronicleMapBuilder.of(String.class,Buyer.class).entries(5000L).replication((byte)200,tcpTransportAndNetworkConfig);
//            buyerChronicleMap=builder.create();
//        }
//        catch (Exception e) {
//            System.out.println("Error(s) creating instrument cache: " + e);
//        }
//        return buyerChronicleMap;
//    }

    public BuyerCache(Collection<String> buyerFiles, CountDownLatch countDownLatch) {
        this.buyerFiles = buyerFiles;
        this.countDownLatch = countDownLatch;
    }

    //读取所有商品文件，按照商品号hash到多个小文件中, 生成到第一块磁盘中
    public void cacheBuyer() {
        System.gc();
        try {
            for (String buyerFile : buyerFiles) {
                FileInputStream buyer_records = new FileInputStream(buyerFile);
                BufferedReader buyer_br = new BufferedReader(new InputStreamReader(buyer_records));

                String str = null;
                int cacheNum = 0;
                while ((str = buyer_br.readLine()) != null) {
                    if (cacheNum >= 1000000) {
                        buyer_br.close();
                        return;
                    }
                    Buyer buyer = new Buyer();
                    StringTokenizer stringTokenizer = new StringTokenizer(str, "\t");
                    while (stringTokenizer.hasMoreElements()) {
                        StringTokenizer keyValue = new StringTokenizer(stringTokenizer.nextToken(), ":");
                        String key = keyValue.nextToken();
                        String value = keyValue.nextToken();
                        KeyValue kv = new KeyValue();
                        kv.setKey(key);
                        kv.setValue(value);
                        buyer.getKeyValues().put(key, kv);
                    }
                    buyer.setId(buyer.getKeyValues().get("buyerid").getValue());
                    //System.out.println("===========================cache good===============");
                    BuyerCache.buyerMap.put(buyer.getId(), buyer);
                    cacheNum++;
                }
                buyer_br.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void run(){
        if (countDownLatch != null) {
            try {
                countDownLatch.await(); //等待构建索引的所有任务的结束
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        System.out.println("cache buyer  start~");
        cacheBuyer();
        System.out.println("cache buyer  end~");
        //countDownLatch.countDown();
    }

    public static void main(String args[]) {
//        createReplicatedMap();
//        Buyer buyer = new Buyer();
//        buyer.setId("jiang");
//        Buyer buyer1 = new Buyer();
//        buyer1.setId("ren");
//        buyerChronicleMap.put("jiangchao", buyer);
//        buyerChronicleMap.put("renmin", buyer1);
//
//        System.out.println(buyerChronicleMap.get("jiangchao").getId());
//        System.out.println(buyerChronicleMap.get("renmin").getId());
        System.out.println(Runtime.getRuntime().freeMemory());
        System.out.println(Runtime.getRuntime().maxMemory());
    }
}