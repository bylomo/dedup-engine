package com.dedup4.dedup.engine.detection.entity;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import com.hazelcast.core.MapStore;

public class Md5PositionStore implements MapStore<String, Long>{

	private static final String CREATE_STORE_TABLE = 
			"CREATE TABLE IF NOT EXISTS md5_position_store "
			+ "(md5_key CHAR(32), "
			+ " node VARCHAR, "
			+ " container_id BIGINT, "
			+ " start_position BIGINT, "
			+ " chunk_size BIGINT)";
	
	private Connection conn;
	
	public Md5PositionStore() {
		try {
			InputStream in = Md5PositionStore.class.getResourceAsStream("");
			
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("");
			conn.createStatement().executeUpdate(CREATE_STORE_TABLE);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Long load(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, Long> loadAll(Collection<String> arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Iterable<String> loadAllKeys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void deleteAll(Collection<String> arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void store(String arg0, Long arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void storeAll(Map<String, Long> arg0) {
		// TODO Auto-generated method stub
		
	}
	
}
