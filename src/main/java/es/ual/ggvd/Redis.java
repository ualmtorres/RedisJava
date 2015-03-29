package es.ual.ggvd;

import java.util.List;
import java.util.Set;

import redis.clients.jedis.BinaryClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.Tuple;

public class Redis {
	// Threadsafe pool of network connections
	JedisPool pool;
	
	// Redis connection
	Jedis jedis;
	
	/**
	 * Returns a direct connection to a Redis database in localhost
	 * using the default port. 
	 *
	 * @return      a connection to Redis
	 * @see         Image
	 */
	public Jedis getDirectConnection() {
		jedis = new Jedis("localhost");
		
		return jedis;
	}
	
	/**
	 * Close a connection to a Redis database
	 *
	 */
	public void closeDirectConnection() {
		if (jedis != null) {
			jedis.close();
		}
	}
	
	/**
	 * Returns a connection from a pool to a Redis database in 
	 * localhost using the default port
	 * 
	 * @return      a connection from a pool to Redis 
	 */
	public Jedis getConnection() {
		pool = new JedisPool(new JedisPoolConfig(), "localhost");
		
		jedis = pool.getResource();
		
		return jedis;
		
	}
	
	/**
	 * Destroys a pool of network connections and close the connection 
	 *
	 */
	public void destroyPool() {

		// Close the connection
		if (jedis != null) {
			jedis.close();
		}
		
		// Destroy the pool
		if (pool != null) {
			pool.destroy();
		}
	}
	
	public void strings() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// SET and GET
		jedis.set("foo", "bar");
		String value = jedis.get("foo");
		System.out.println(value);
		
		// MSET and MGET 
		jedis.mset("a", "10", "b", "20", "c", "30");
		List<String> values= jedis.mget("a", "b", "c");
		for (String v: values) {
			System.out.println(v);
		}
		
		// Delete the keys created
		jedis.del("foo", "a", "b", "c");
		
		// Close the connection
		jedis.close();
	}
	
	public void numbers() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Setup the key
		jedis.set("counter", "100");
		
		// INCR, INCRBY, DECR and DECRBY
		jedis.incr("counter");
		jedis.incrBy("counter", 9);
		jedis.decrBy("counter", 4);
		jedis.decr("counter");
		
		// Prints the value of the key
		System.out.println("counter: " + jedis.get("counter"));
		
		// Delete the key created
		jedis.del("counter");
		
		// Close the connection
		jedis.close();
	}
	
	public void moreStrings() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Setup the key
		jedis.set("greeting", "Hello ");
		
		// EXISTS and APPEND
		if (jedis.exists("greeting")) {
			jedis.append("greeting", "World!");
		}
		System.out.println("Appended greeting: " + jedis.get("greeting"));
		
		// SUBSTR and STRLEN
		System.out.println("Substring: " + jedis.substr("greeting", 6, -1));
		System.out.println("greeting: " + jedis.get("greeting"));
		System.out.println("Length: " + jedis.strlen("greeting"));
		
		// Delete the key created
		jedis.del("greeting");
		
		// Close the connection
		jedis.close();
	}
	
	public void lists() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Delete the key to avoid unexpected results
		jedis.del("sessions:ggvd");
		
		// LPUSH, RPUSH, RPOP, LINSERT and LSET
		jedis.lpush("sessions:ggvd", "10/3");
		jedis.rpush("sessions:ggvd", "24/3");
		jedis.rpush("sessions:ggvd", "25/3");
		jedis.rpop("sessions:ggvd");
		jedis.linsert("sessions:ggvd", BinaryClient.LIST_POSITION.BEFORE, "24/3", "17/3");
		jedis.rpush("sessions:ggvd", "31/3");
		jedis.lset("sessions:ggvd", -1, "7/4");

		// Obtain all the values of the lists
		List<String> values = jedis.lrange("sessions:ggvd", 0, -1);
		
		// Print the list
		for (String v: values) {
			System.out.println(v);
		}
		
		// Delete the key created
		jedis.del("sessions:ggvd");
		
		// Close the connection
		jedis.close();
	}
	
	public void sets() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Delete the key to avoid unexpected results
		jedis.del("students:ggvd");
		
		// SADD, SREM, SCARD and SMEMBERS
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.srem("students:ggvd", "student3");
		System.out.println(jedis.scard("students:ggvd") + " elements");
		Set<String> students = jedis.smembers("students:ggvd");
		
		// Print the list
		for (String student: students) {
			System.out.println(student);
		}
		
		// Delete the key created
		jedis.del("students:ggvd");
		
		// Close the connection
		jedis.close();
	}
	
	public void setOperations() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Delete the keys to avoid unexpected results
		jedis.del("students:bd");
		jedis.del("students:ggvd");
		
		// Setup the sets
		jedis.sadd("students:ggvd", "student1", "student2", "student3");
		jedis.sadd("students:bd", "student3", "student4", "student5");
		
		// SUNION, SINTER and SDIFF 
		Set<String> totalStudents = jedis.sunion("students:bd", "students:ggvd");
		Set<String> commonStudents = jedis.sinter("students:bd", "students:ggvd");
		Set<String> studentsOnlyInGGVD = jedis.sdiff("students:ggvd", "students:bd");
		
		// Print the union
		System.out.println("*** Total students:");
		for (String s: totalStudents) {
			System.out.println(s);
		}
		
		// Print the intersection
		System.out.println("*** Common students:");
		for (String s: commonStudents) {
			System.out.println(s);
		}

		// Print the difference
		System.out.println("*** Students only in GGVD:");
		for (String s: studentsOnlyInGGVD) {
			System.out.println(s);
		}

		// Delete the keys created
		jedis.del("students:bd");
		jedis.del("students:ggvd");
		
		// Close the connection
		jedis.close();
	}
	
	public void sortedSets() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Delete the key to avoid unexpected results
		jedis.del("scores:ggvd");
		
		// ZADD
		jedis.zadd("scores:ggvd", 9, "student1");
		jedis.zadd("scores:ggvd", 3, "student2");
		jedis.zadd("scores:ggvd", 8, "student3");
		
		// ZINCRBY
		jedis.zincrby("scores:ggvd", 1, "student2");
		
		// ZCOUNT
		long numberOfPassStudents = jedis.zcount("scores:ggvd", 5, 10);
		
		// ZRANGEBYSCOREWITHSCORES
		Set<Tuple> passedStudents = jedis.zrangeByScoreWithScores("scores:ggvd", 5, 10);
		
		// Print the results
		System.out.println("*** Number of passed students: " + numberOfPassStudents);
		
		for (Tuple s: passedStudents) {
			System.out.println(s.getElement() + " " + s.getScore());
		}
		
		// Delete the key created
		jedis.del("scores:ggvd");
		
		// Close the connection
		jedis.close();
	}
	
	public void hashes() {
		// Create a connection
		Jedis jedis = new Jedis("localhost");
		
		// Delete the key to avoid unexpected results
		jedis.del("user:mtorres");
		
		// HSET
		jedis.hset("user:mtorres", "email", "mtorres@ual.es");
		jedis.hset("user:mtorres", "name", "Manuel");
		jedis.hset("user:mtorres", "surname", "Torres Gil");
		jedis.hset("user:mtorres", "twitter", "@ualmtorres");
		
		// HKEYS
		Set<String> keys = jedis.hkeys("user:mtorres");
		
		// Print the results
		for (String c: keys) {
			System.out.println(c + ": " + jedis.hget("user:mtorres", c));
		}
		
		// Delete the key created
		jedis.del("user:mtorres");
		
		// Close the connection
		jedis.close();
	}
	
	public void transactions() {
		Jedis jedis = new Jedis("localhost");
		
		// Transaction commiting results
		Transaction t = jedis.multi();
		t.set("a", "1");
		t.set("b", "2");
		t.exec();
		
		// Transaction discarding results
		t = jedis.multi();
		t.set("a", "3");
		t.set("b", "4");
		t.discard();

		System.out.println("*** Keys after discarding: ");
		System.out.println(jedis.get("a"));
		System.out.println(jedis.get("b"));
		
		// Delete the keys created
		jedis.del("a", "b");
		
		// Close the connection
		jedis.close();
	}
}
