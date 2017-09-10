package com.magdicgoran;

import com.mongodb.MongoClient;

public class MongoTestUtil {

    public static void dropDatabase(String name) {
        new MongoClient().getDatabase(name).drop();
    }

}
