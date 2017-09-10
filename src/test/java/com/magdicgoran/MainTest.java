package com.magdicgoran;

import com.mongodb.Block;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.mongodb.client.model.Accumulators.avg;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.*;

public class MainTest {

    private static final String GDEV_DRIVER = "gdev_driver";
    private static final String MOLECULES = "molecules";
    private MongoClient mongoClient = new MongoClient();
    private MongoDatabase database = mongoClient.getDatabase(GDEV_DRIVER);
    private MongoCollection<Document> moleculesCollection = database.getCollection(MOLECULES);

    @Before
    public void setUp() throws Exception {
        database.drop();
    }

    @Test
    public void insertOne() throws Exception {
        Document waterDocument = new Document("name", "water")
                .append("formula", "H2O");

        moleculesCollection.insertOne(waterDocument);

        moleculesCollection.find().forEach((Block<? super Document>) System.out::println);

        Assert.assertEquals(1, moleculesCollection.count());

    }

    @Test
    public void insertMany() throws Exception {
        List<Document> newMolecules = Arrays.asList(
                new Document("name", "Water").append("formula", "H2O2"),
                new Document("name", "Hydrogen Peroxide").append("formula", "H2O2"),
                new Document("name", "Carbon Monoxide").append("formula", "CO"),
                new Document("name", "Carbon Dioxide").append("formula", "CO2"));

        moleculesCollection.insertMany(newMolecules);
        moleculesCollection.find().forEach((Block<? super Document>) System.out::println);
        Assert.assertEquals(4, moleculesCollection.count());
    }


    @Test
    public void update() throws Exception {

        moleculesCollection.insertMany(Arrays.asList(
                new Document("name", "Water").append("formula", "H2O2"),
                new Document("name", "Hydrogen Peroxide").append("formula", "H2O2"),
                new Document("name", "Carbon Monoxide").append("formula", "CO"),
                new Document("name", "Carbon Dioxide").append("formula", "CO2")));

        moleculesCollection.updateMany(
                        eq("name", "Water"),
                        new Document("$set",new Document("formula", "H2O"))
                );

        moleculesCollection.find().forEach((Block<? super Document>) System.out::println);

        Assert.assertEquals(4, moleculesCollection.count());

    }


    @Test
    public void find() throws Exception {

        moleculesCollection.insertMany(Arrays.asList(
                new Document("name", "Water").append("formula", "H2O2").append("mass", 18),
                new Document("name", "Hydrogen Peroxide").append("formula", "H2O2").append("mass", 34),
                new Document("name", "Carbon Monoxide").append("formula", "CO").append("mass", 28),
                new Document("name", "Carbon Dioxide").append("formula", "CO2").append("mass", 44)));

        List<Document> findResult = moleculesCollection.find(
                or(
                        gt("mass", 30),
                        lt("mass", 20)
                )
        ).into(new ArrayList<>());

        findResult.forEach(System.out::println);
        Assert.assertEquals(3, findResult.size());

    }

    @Test
    public void aggregate() throws Exception {

        moleculesCollection.insertMany(Arrays.asList(
                new Document("name", "Water")
                        .append("formula", "H2O2")
                        .append("atoms", Arrays.asList("H", "O"))
                        .append("mass", 18),
                new Document("name", "Hydrogen Peroxide")
                        .append("formula", "H2O2")
                        .append("atoms", Collections.singletonList("O"))
                        .append("mass", 34),
                new Document("name", "Carbon Monoxide")
                        .append("formula", "CO")
                        .append("atoms", Arrays.asList("C", "O"))
                        .append("mass", 28),
                new Document("name", "Carbon Dioxide")
                        .append("formula", "CO2")
                        .append("atoms", Arrays.asList("C", "O"))
                        .append("mass", 44)));

        //([{"$unwind" :"$atoms"}, {"$group":{_id:"$atoms", avgMass: {$avg:"$mass"}}}])
        List<Document> findResult = moleculesCollection.aggregate(
                Arrays.asList(
                        unwind("$atoms"),
                        group("$atoms", avg("averageMass", "$mass"))
                )
        ).into(new ArrayList<>());

        findResult.forEach(System.out::println);
        Assert.assertEquals(3, findResult.size());
    }


    @Test
    public void aggregateSomeMore() throws Exception {


        moleculesCollection.insertMany(Arrays.asList(
                new Document("name", "Water")
                        .append("formula", "H2O2")
                        .append("atoms", Arrays.asList("H", "O"))
                        .append("mass", 18),
                new Document("name", "Hydrogen Peroxide")
                        .append("formula", "H2O2")
                        .append("atoms", Collections.singletonList("O"))
                        .append("mass", 34),
                new Document("name", "Carbon Monoxide")
                        .append("formula", "CO")
                        .append("atoms", Arrays.asList("C", "O"))
                        .append("mass", 28),
                new Document("name", "Carbon Dioxide")
                        .append("formula", "CO2")
                        .append("atoms", Arrays.asList("C", "O"))
                        .append("mass", 44)));

        List<Document> findResult = moleculesCollection.aggregate(
                Arrays.asList(
                        match(ne("name", "Hydrogen Peroxide")),
                        unwind("$atoms"),
                        group("$atoms",
                                avg("averageMass", "$mass"),
                                sum("count", 1)
                        )
                )
        ).into(new ArrayList<>());

        findResult.forEach(System.out::println);
        Assert.assertEquals(3, findResult.size());
    }

}