package es.ual.ggvd;

public class RedisTester {

	public static void main(String[] args) {
		Redis redis = new Redis();
		redis.strings();
		redis.numbers();
		redis.moreStrings();
		redis.lists();
		redis.sets();
		redis.setOperations();
		redis.sortedSets();
		redis.hashes();
		redis.transactions();
	}

}
