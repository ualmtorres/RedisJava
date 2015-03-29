package es.ual.ggvd;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class OperationsTest {

	Redis redis;
	Jedis jedis;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		redis = new Redis();
		
		jedis = redis.getConnection();
	}

	@After
	public void tearDown() throws Exception {
		redis.destroyPool();
	}
	
	@Test
	public void shouldSetAndGet() {
		
		jedis.set("foo", "bar");
		String value = jedis.get("foo");
		
		assertEquals(value, "bar");

		jedis.del("foo");
	}	

	@Test
	public void shouldMSetAndMGet() {

		String[] originalValues = {"10", "20", "30"};

		jedis.mset("a", originalValues[0], 
				"b", originalValues[1], 
				"c", originalValues[2]);

		List<String> values= jedis.mget("a", "b", "c");

		int i = 0;
		
		for (String v: values) {
			assertEquals(v, originalValues[i++]);
		}
		
		jedis.del("a", "b", "c");

	}
	
	@Test
	public void shouldOperateWithNumbers() {

		jedis.set("counter", "100");
		jedis.incr("counter");
		jedis.incrBy("counter", 9);
		jedis.decrBy("counter", 4);
		jedis.decr("counter");
		
		assertEquals(jedis.get("counter"), "105");

		jedis.del("counter");
	}	
	
	@Test
	public void shouldAppendStrings() {
		
		jedis.set("greeting", "Hello ");
		jedis.append("greeting", "World!");
		
		assertEquals(jedis.get("greeting"), "Hello World!");

		jedis.del("greeting");
		
	}
	
	@Test
	public void shouldExtractSubstrings() {
		
		jedis.set("greeting", "Hello World!");
		String subcadena = jedis.substr("greeting", 6, -1);
		
		assertEquals(subcadena, "World!");

		jedis.del("greeting");

	}
	
	@Test
	public void shouldObtainStringLenth() {
		
		jedis.set("greeting", "Hello World!");
		long longitud = jedis.strlen("greeting");
		
		assertEquals(longitud, 12);

		jedis.del("greeting");

	}
	
	@Test
	public void shouldWorkWithLists() {
		
		String[] finalValues = {"10/3", "17/3", "24/3", "7/4"};
		
		jedis.del("sessions:ggvd");
		
		jedis.lpush("sessions:ggvd", "10/3");
		jedis.rpush("sessions:ggvd", "24/3");
		jedis.rpush("sessions:ggvd", "25/3");
		jedis.rpop("sessions:ggvd");
		jedis.linsert("sessions:ggvd", BinaryClient.LIST_POSITION.BEFORE, "24/3", "17/3");
		jedis.rpush("sessions:ggvd", "31/3");
		jedis.lset("sessions:ggvd", -1, "7/4");
		
		List<String> values = jedis.lrange("sessions:ggvd", 0, -1);

		int i = 0;
		
		for (String v: values) {
			assertEquals(v, finalValues[i++]);
		}
		
		jedis.del("sessions:ggvd");
	}
	
	@Test
	public void shouldHaveASetWithTwoStudents() {
		
		jedis.del("students:ggvd");
		
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.srem("students:ggvd", "student3");
		
		long numberOfStudents = jedis.scard("students:ggvd");
		
		assertEquals(numberOfStudents, 2);
		
		jedis.del("students:ggvd");
	}
	
	@Test
	public void shouldHaveASetWithoutDeletedStudent() {
		
		jedis.del("students:ggvd");
		
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.srem("students:ggvd", "student3");
		
		Set<String> students = jedis.smembers("students:ggvd");
		
		for (String s: students) {
			assertTrue(jedis.sismember("students:ggvd", s));
		}		

		long numberOfStudents = jedis.scard("students:ggvd");

		assertEquals(numberOfStudents, 2);


		jedis.del("students:ggvd");
	}
	
	@Test
	public void shouldMakeTheUnionOfSets() {
		
		jedis.del("students:bd");
		jedis.del("students:ggvd");
		jedis.del("students:unionSet");
		
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.sadd("students:bd", "student3", "student4", "student5");
		
		Set<String> resultingStudents = jedis.sunion("students:bd", "students:ggvd");
		jedis.sunionstore("students:unionSet", "students:bd", "students:ggvd");
		
		for (String s: resultingStudents) {
			assertTrue(jedis.sismember("students:unionSet", s));
		}

		long numberOfStudents = jedis.scard("students:unionSet");

		assertEquals(numberOfStudents, 5);

		jedis.del("students:bd");
		jedis.del("students:ggvd");
		jedis.del("students:unionSet");
	}	

	@Test
	public void shouldMakeTheIntersectOfSets() {
		
		jedis.del("students:bd");
		jedis.del("students:ggvd");
		jedis.del("students:intersectSet");
		
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.sadd("students:bd", "student3", "student4", "student5");
		
		Set<String> resultingStudents = jedis.sinter("students:bd", "students:ggvd");
		jedis.sinterstore("students:intersectSet", "students:bd", "students:ggvd");
		
		for (String s: resultingStudents) {
			assertTrue(jedis.sismember("students:intersectSet", s));
		}

		long numberOfStudents = jedis.scard("students:intersectSet");

		assertEquals(numberOfStudents, 1);

		jedis.del("students:bd");
		jedis.del("students:ggvd");
		jedis.del("students:intersectSet");
	}	

	@Test
	public void shouldMakeTheDifferenceOfSets() {
		
		jedis.del("students:bd");
		jedis.del("students:ggvd");
		jedis.del("students:diffSet");
		
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.sadd("students:bd", "student3", "student4", "student5");
		
		Set<String> resultingStudents = jedis.sdiff("students:bd", "students:ggvd");
		jedis.sdiffstore("students:diffSet", "students:bd", "students:ggvd");
		
		for (String s: resultingStudents) {
			assertTrue(jedis.sismember("students:diffSet", s));
		}

		long numberOfStudents = jedis.scard("students:diffSet");

		assertEquals(numberOfStudents, 2);

		jedis.del("students:bd");
		jedis.del("students:ggvd");
		jedis.del("students:diffSet");
	}	
	
	@Test
	public void shouldWorkWithZSets() {
		
		jedis.del("scores:ggvd");
		
		jedis.zadd("scores:ggvd", 9, "student1");
		jedis.zadd("scores:ggvd", 3, "student2");
		jedis.zadd("scores:ggvd", 8, "student3");
		
		jedis.zincrby("scores:ggvd", 1, "student2");
		
		long numberOfPassedStudents = jedis.zcount("scores:ggvd", 5, 10);
		
		// TODO 
		assertEquals(numberOfPassedStudents, 2);
		assertEquals(jedis.zscore("scores:ggvd", "student3").intValue(), 8);
		assertEquals(jedis.zscore("scores:ggvd", "student1").intValue(), 9);
				
		jedis.del("scores:ggvd");
	}
	
	@Test
	public void shouldWorkWithHashes() {
		
		String key = "profesor:mtorres";
		String[] keys = {"email", "nombre", "apellidos", "twitter"};
		String[] values = {"mtorres@ual.es", "Manuel", "Torres Gil", "@ualmtorres"};
		
		jedis.del(key);
		
		for (int i = 0; i < keys.length; i++) {
			jedis.hset(key, keys[i], values[i]);
		}

		for (int i = 0; i < keys.length; i++) {
			assertEquals(jedis.hget(key, keys[i]), values[i]);
		}
		
		assertTrue(jedis.hlen(key) == (long)keys.length);
				
		Set<String> hashKeys = jedis.hkeys(key);
		
		for (String k: hashKeys) {
			assertTrue(jedis.hexists(key, k));
		}
				
		jedis.del(key);
	}
	
	@Test
	public void shouldExecTransactions() {
		
		String[] keys = {"a", "b"};
		String[] values = {"1", "2"};

		for (String k: keys) {
			jedis.del(k);
		}
		
		Transaction t = jedis.multi();
		
		for (int i = 0; i < keys.length; i++) {
			t.set(keys[i], values[i]);
		}
		
		t.exec();
		
		for (int i = 0; i < keys.length; i++) {
			assertEquals(jedis.get(keys[i]), values[i]);
		}
		
		for (String k: keys) {
			jedis.del(k);
		}
	}
	
	@Test
	public void shouldDiscardTransactions() {
		
		String[] keys = {"a", "b"};
		String[] values = {"1", "2"};

		for (String k: keys) {
			jedis.del(k);
		}
		
		Transaction t = jedis.multi();
		
		for (int i = 0; i < keys.length; i++) {
			t.set(keys[i], values[i]);
		}
		
		t.discard();
		
		for (int i = 0; i < keys.length; i++) {
			assertNull(jedis.get(keys[i]));
		}
		
		for (String k: keys) {
			jedis.del(k);
		}
	}
}
