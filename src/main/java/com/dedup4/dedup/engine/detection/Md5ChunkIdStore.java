package com.dedup4.dedup.engine.detection;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.hazelcast.core.MapStore;







import static java.lang.String.format;

public class Md5ChunkIdStore implements MapStore<String, Long>{

	private static final String DEFAULT_DRIVER_NAME = "com.mysql.jdbc.Driver";
	private static final String DEFAULT_HOST = "127.0.0.1";
	private static final int DEFAULT_PORT = 3306;
	private static final String DEFAULT_DB_NAME = "dedup";
	private static final String DEFAULT_TB_NAME = "md5_position_store";
	private static final String DEFAULT_USERNAME = "root";
	private static final String DEFAULT_PASSWORD = "";
	
	private static final String CREATE_STORE_TABLE = 
			"CREATE TABLE IF NOT EXISTS md5_position_store "
			+ "(key CHAR(32) NOT NULL, "
			+ " value INTEGER,"
			+ " primary key(key))";
	
	private Connection conn;
	private String driverName;
	private String host;
	private int port;
	private String dbName;
	private String tbName;
	private String userName;
	private String password;
	
	private PreparedStatement allKeysStatement;
	
	public Md5ChunkIdStore() {
		loadProperties();
		initConnection();
		initAllKeys();
	}
	
	@Override
	public  synchronized Long load(String key) {
		String sql = format("SELECT value FROM %s WHERE key = '%s'", tbName, key);
		
		try {
            ResultSet resultSet = conn.createStatement().executeQuery(sql);
            try {
                if (!resultSet.next()) {
                    return null;
                }
                return resultSet.getLong(1);
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}

	@Override
	public  synchronized  Map<String, Long> loadAll(Collection<String> keys) {
		Map<String, Long> result = new HashMap<String, Long>();
        for (String key : keys) {
            result.put(key, load(key));
        }
        return result;
	}

	@Override
	public Iterable<String> loadAllKeys() {
		 return new StatementIterable<String>(allKeysStatement);
	}

	@Override
	public synchronized void delete(String key) {
		String sql = format("DELETE FROM %s WHERE key = '%s'", tbName, key);
		executeUpdate(sql);
	}

	@Override
	public  synchronized void deleteAll(Collection<String> keys) {
		for (String key : keys) {
            delete(key);
        }
	}

	@Override
	public  synchronized void store(String key, Long value) {
		String sql = format("INSERT INTO %s VALUES('%s', %s)", tbName, key, value);
		executeUpdate(sql);
	}

	@Override
	public  synchronized void storeAll(Map<String, Long> map) {
		for (Map.Entry<String, Long> entry : map.entrySet()) {
            store(entry.getKey(), entry.getValue());
        }
	}
	
	private void executeUpdate(String sql) {
		try {
            conn.createStatement().executeUpdate(sql);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
	}
	
	private void loadProperties() {
		Properties mysqlProperties = new Properties();
		InputStream in = getClass().getResourceAsStream("/mysql.properties");
		try {
			mysqlProperties.load(in);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		String driverNameStr = (String)mysqlProperties.get("driverName");
		driverName = (driverNameStr == null ? DEFAULT_DRIVER_NAME : driverNameStr);
		
		String hostStr = (String)mysqlProperties.get("host");
		host = (hostStr == null ? DEFAULT_HOST : hostStr);
		
		String portStr = (String)mysqlProperties.get("port");
		port = (portStr == null ? DEFAULT_PORT : Integer.valueOf(portStr));
		
		String dbNameStr = (String)mysqlProperties.get("dbName");
		dbName = (dbNameStr == null ? DEFAULT_DB_NAME : dbNameStr);
		
		String tbNameStr = (String)mysqlProperties.get("tbName");
		tbName = (tbNameStr == null ? DEFAULT_TB_NAME : tbNameStr);
		
		String userNameStr = (String)mysqlProperties.get("userName");
		userName = (userNameStr == null ? DEFAULT_USERNAME : userNameStr);
		
		String passwordStr = (String)mysqlProperties.get("password");
		password = (passwordStr == null ? DEFAULT_PASSWORD : passwordStr);
	}
	
	private void initConnection() {
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(formatUrl());
			conn.createStatement().executeUpdate(CREATE_STORE_TABLE);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	private void initAllKeys() {
		try {
			allKeysStatement = conn.prepareStatement("select id from person");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private String formatUrl() {
		return "jdbc:mysql://" + host + ":" + port + "/" + dbName + "?user=" + userName + "&password=" + password; 
	}
}
