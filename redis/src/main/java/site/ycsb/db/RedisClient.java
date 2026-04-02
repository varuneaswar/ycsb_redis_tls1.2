/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package site.ycsb.db;

import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import redis.clients.jedis.commands.BasicCommands;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for <a href="http://redis.io/">Redis</a>.
 *
 * See {@code redis/README.md} for details.
 */
public class RedisClient extends DB {

  private Jedis jedis;
  private JedisCluster jedisCluster;
  private int expireSeconds;

  public static final String HOST_PROPERTY = "redis.host";
  public static final String PORT_PROPERTY = "redis.port";
  public static final String PASSWORD_PROPERTY = "redis.password";
  public static final String USERNAME_PROPERTY = "redis.username";
  public static final String CLUSTER_PROPERTY = "redis.cluster";
  public static final String TIMEOUT_PROPERTY = "redis.timeout";
  public static final String SSL_PROPERTY = "redis.ssl";
  public static final String SSL_KEYSTORE_PATH_PROPERTY = "redis.ssl.keystore.path";
  public static final String SSL_KEYSTORE_PASSWORD_PROPERTY = "redis.ssl.keystore.password";
  public static final String SSL_TRUSTSTORE_PATH_PROPERTY = "redis.ssl.truststore.path";
  public static final String SSL_TRUSTSTORE_PASSWORD_PROPERTY = "redis.ssl.truststore.password";
  public static final String EXPIRE_PROPERTY = "redis.expire";
  public static final int EXPIRE_PROPERTY_DEFAULT = 0;

  public static final String INDEX_KEY = "_indices";

  public void init() throws DBException {
    Properties props = getProperties();
    int port;

    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = Protocol.DEFAULT_PORT;
    }
    String host = props.getProperty(HOST_PROPERTY);

    boolean sslEnabled = Boolean.parseBoolean(props.getProperty(SSL_PROPERTY, "false"));
    String password = props.getProperty(PASSWORD_PROPERTY);
    String username = props.getProperty(USERNAME_PROPERTY);
    String redisTimeout = props.getProperty(TIMEOUT_PROPERTY);
    int timeout = redisTimeout != null ? Integer.parseInt(redisTimeout) : Protocol.DEFAULT_TIMEOUT;
    expireSeconds = Integer.parseInt(
        props.getProperty(EXPIRE_PROPERTY, String.valueOf(EXPIRE_PROPERTY_DEFAULT)));

    SSLSocketFactory sslSocketFactory = null;
    if (sslEnabled) {
      String keystorePath = props.getProperty(SSL_KEYSTORE_PATH_PROPERTY);
      String keystorePassword = props.getProperty(SSL_KEYSTORE_PASSWORD_PROPERTY);
      String truststorePath = props.getProperty(SSL_TRUSTSTORE_PATH_PROPERTY);
      String truststorePassword = props.getProperty(SSL_TRUSTSTORE_PASSWORD_PROPERTY);
      if (keystorePath != null || truststorePath != null) {
        try {
          SSLContext sslContext = buildSSLContext(
              keystorePath, keystorePassword, truststorePath, truststorePassword);
          sslSocketFactory = sslContext.getSocketFactory();
        } catch (Exception e) {
          throw new DBException("Failed to create SSL context"
              + " (keystore=" + keystorePath + ", truststore=" + truststorePath + "): "
              + e.getMessage());
        }
      }
    }

    boolean clusterEnabled = Boolean.parseBoolean(props.getProperty(CLUSTER_PROPERTY));
    if (clusterEnabled) {
      Set<HostAndPort> jedisClusterNodes = new HashSet<>();
      jedisClusterNodes.add(new HostAndPort(host, port));
      JedisPoolConfig poolConfig = new JedisPoolConfig();
      if (sslEnabled) {
        if (username != null) {
          jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout,
              JedisCluster.DEFAULT_MAX_ATTEMPTS, username, password, null, poolConfig,
              true, sslSocketFactory, null, null, null);
        } else {
          jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout,
              JedisCluster.DEFAULT_MAX_ATTEMPTS, password, null, poolConfig,
              true, sslSocketFactory, null, null, null);
        }
      } else if (password != null) {
        if (username != null) {
          jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout,
              JedisCluster.DEFAULT_MAX_ATTEMPTS, username, password, null, poolConfig);
        } else {
          jedisCluster = new JedisCluster(jedisClusterNodes, timeout, timeout,
              JedisCluster.DEFAULT_MAX_ATTEMPTS, password, poolConfig);
        }
      } else {
        jedisCluster = new JedisCluster(jedisClusterNodes);
      }
    } else {
      if (sslEnabled) {
        jedis = new Jedis(host, port, timeout, timeout,
            true, sslSocketFactory, null, null);
      } else {
        jedis = new Jedis(host, port, timeout);
      }
      jedis.connect();
      if (password != null) {
        if (username != null) {
          ((BasicCommands) jedis).auth(username, password);
        } else {
          ((BasicCommands) jedis).auth(password);
        }
      }
    }
  }

  private static SSLContext buildSSLContext(String keystorePath, String keystorePassword,
      String truststorePath, String truststorePassword) throws Exception {
    KeyManagerFactory kmf = null;
    if (keystorePath != null) {
      KeyStore keyStore = KeyStore.getInstance("JKS");
      char[] ksPass = keystorePassword != null ? keystorePassword.toCharArray() : new char[0];
      try (FileInputStream fis = new FileInputStream(keystorePath)) {
        keyStore.load(fis, ksPass);
      }
      kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(keyStore, ksPass);
    }

    TrustManagerFactory tmf = null;
    if (truststorePath != null) {
      KeyStore trustStore = KeyStore.getInstance("JKS");
      char[] tsPass = truststorePassword != null ? truststorePassword.toCharArray() : new char[0];
      try (FileInputStream fis = new FileInputStream(truststorePath)) {
        trustStore.load(fis, tsPass);
      }
      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(trustStore);
    }

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        kmf != null ? kmf.getKeyManagers() : null,
        tmf != null ? tmf.getTrustManagers() : null,
        null);
    return sslContext;
  }

  public void cleanup() throws DBException {
    try {
      if (jedisCluster != null) {
        jedisCluster.close();
      } else if (jedis != null) {
        jedis.close();
      }
    } catch (Exception e) {
      throw new DBException("Closing connection failed: " + e.getMessage());
    }
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  private String hmset(String key, Map<String, String> hash) {
    return jedisCluster != null ? jedisCluster.hmset(key, hash) : jedis.hmset(key, hash);
  }

  private List<String> hmget(String key, String... fields) {
    return jedisCluster != null ? jedisCluster.hmget(key, fields) : jedis.hmget(key, fields);
  }

  private Map<String, String> hgetAll(String key) {
    return jedisCluster != null ? jedisCluster.hgetAll(key) : jedis.hgetAll(key);
  }

  private Long zadd(String key, double score, String member) {
    return jedisCluster != null ? jedisCluster.zadd(key, score, member) : jedis.zadd(key, score, member);
  }

  private Set<String> zrangeByScore(String key, double min, double max, int offset, int count) {
    return jedisCluster != null
        ? jedisCluster.zrangeByScore(key, min, max, offset, count)
        : jedis.zrangeByScore(key, min, max, offset, count);
  }

  private Long del(String key) {
    return jedisCluster != null ? jedisCluster.del(key) : jedis.del(key);
  }

  private Long zrem(String key, String member) {
    return jedisCluster != null ? jedisCluster.zrem(key, member) : jedis.zrem(key, member);
  }

  private Long expire(String key, int seconds) {
    return jedisCluster != null ? jedisCluster.expire(key, seconds) : jedis.expire(key, seconds);
  }

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    if (fields == null) {
      StringByteIterator.putAllAsByteIterators(result, hgetAll(key));
    } else {
      String[] fieldArray = fields.toArray(new String[fields.size()]);
      List<String> values = hmget(key, fieldArray);

      Iterator<String> fieldIterator = fields.iterator();
      Iterator<String> valueIterator = values.iterator();

      while (fieldIterator.hasNext() && valueIterator.hasNext()) {
        result.put(fieldIterator.next(),
            new StringByteIterator(valueIterator.next()));
      }
      assert !fieldIterator.hasNext() && !valueIterator.hasNext();
    }
    return result.isEmpty() ? Status.ERROR : Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    if (hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
      zadd(INDEX_KEY, hash(key), key);
      if (expireSeconds > 0) {
        expire(key, expireSeconds);
      }
      return Status.OK;
    }
    return Status.ERROR;
  }

  @Override
  public Status delete(String table, String key) {
    return del(key) == 0 && zrem(INDEX_KEY, key) == 0 ? Status.ERROR : Status.OK;
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    if (!hmset(key, StringByteIterator.getStringMap(values)).equals("OK")) {
      return Status.ERROR;
    }
    if (expireSeconds > 0) {
      expire(key, expireSeconds);
    }
    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    Set<String> keys = zrangeByScore(INDEX_KEY, hash(startkey), Double.POSITIVE_INFINITY, 0, recordcount);

    HashMap<String, ByteIterator> values;
    for (String key : keys) {
      values = new HashMap<String, ByteIterator>();
      read(table, key, fields, values);
      result.add(values);
    }

    return Status.OK;
  }

}
