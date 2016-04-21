package com.dedup4.dedup.engine.detection;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class Md5ChunkIdStoreTest {

    public static void main(String[] args) {
        int numberOfEntriesToAdd = 1000;
        String mapName = "md5_chunkId_map";

        Config config = createNewConfig(mapName);
        HazelcastInstance node = Hazelcast.newHazelcastInstance(config);

        IMap<String, Long> map = node.getMap(mapName);

        populateMap(map, numberOfEntriesToAdd);

        System.out.printf("# Map store has %d elements\n", numberOfEntriesToAdd);

        map.evictAll();

        System.out.printf("# After evictAll map size: %d\n", map.size());

        map.loadAll(true);

        System.out.printf("# After loadAll map size: %d\n", map.size());
    }

    private static void populateMap(IMap<String, Long> map, int itemCount) {
        for (long i = 0; i < itemCount; i++) {
            map.put("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + i, i);
        }
    }

    private static Config createNewConfig(String mapName) {
    	Md5ChunkIdStore md5PositionStore = new Md5ChunkIdStore();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation(md5PositionStore);
        mapStoreConfig.setWriteDelaySeconds(0);

        XmlConfigBuilder configBuilder = new XmlConfigBuilder();
        Config config = configBuilder.build();
        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        return config;
    }
}