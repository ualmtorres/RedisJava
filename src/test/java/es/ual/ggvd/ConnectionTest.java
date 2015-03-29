package es.ual.ggvd;

import static org.junit.Assert.*;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import redis.clients.jedis.Jedis;

public class ConnectionTest {
	
	Redis redis;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		redis = new Redis();
	}

	@After
	public void tearDown() throws Exception {
		redis.closeDirectConnection();
	}

	@Test
	public void shouldCreateANewRedisClientConnectedToLocalhost() {
		
		Jedis redisConnection = redis.getDirectConnection();
		
		assertThat(redisConnection, is(notNullValue()));	
		
	}
	
	@Test
	public void shouldCreateANewRedisClientFromPoolConnectedToLocalhost() {
		
		Jedis redisConnection = redis.getConnection();
		
		assertThat(redisConnection, is(notNullValue()));
		
	}
}
