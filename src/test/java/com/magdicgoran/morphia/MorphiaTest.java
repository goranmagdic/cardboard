package com.magdicgoran.morphia;


import com.magdicgoran.MongoTestUtil;
import com.mongodb.AggregationOptions;
import com.mongodb.MongoClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.morphia.Datastore;
import org.mongodb.morphia.Morphia;
import org.mongodb.morphia.aggregation.Group;
import org.mongodb.morphia.query.Query;
import org.mongodb.morphia.query.UpdateOperations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.mongodb.morphia.aggregation.Accumulator.accumulator;
import static org.mongodb.morphia.aggregation.Group.average;
import static org.mongodb.morphia.aggregation.Group.grouping;

public class MorphiaTest {

    private static final String GDEV_MORPHIA = "gdev_morphia";
    private Morphia morphia = new Morphia();
    private Datastore datastore = morphia.createDatastore(new MongoClient(), GDEV_MORPHIA);

    @Before
    public void setUp() throws Exception {
        morphia.map(Molecule.class);
        MongoTestUtil.dropDatabase(GDEV_MORPHIA);
    }

    @Test
    public void insertOne() throws Exception {
        Molecule molecule = Molecule.info("Water", "H2O");

        datastore.save(molecule);

        List<Molecule> molecules = datastore.createQuery(Molecule.class).asList();
        molecules.forEach(System.out::println);
        Assert.assertEquals(1, molecules.size());
    }

    @Test
    public void insertMany() throws Exception {

        datastore.save(Arrays.asList(
                Molecule.info("Water", "H2O2"),
                Molecule.info("Hydrogen Peroxide", "H2O2"),
                Molecule.info("Carbon Monoxide", "CO"),
                Molecule.info("Carbon Dioxide", "CO2")
        ));

        List<Molecule> molecules = datastore.createQuery(Molecule.class).asList();
        molecules.forEach(System.out::println);
        Assert.assertEquals(4, molecules.size());
    }


    @Test
    public void updateByFindAndSave() throws Exception {

        datastore.save(Arrays.asList(
                Molecule.info("Water", "H2O2"),
                Molecule.info("Hydrogen Peroxide", "H2O2"),
                Molecule.info("Carbon Monoxide", "CO"),
                Molecule.info("Carbon Dioxide", "CO2")
        ));

        Molecule water = datastore.createQuery(Molecule.class)
                .filter("name", "Water")
                .asList().get(0);

        water.setFormula("H2O");
        datastore.save(water);

        List<Molecule> molecules = datastore.createQuery(Molecule.class).asList();
        molecules.forEach(System.out::println);
        Assert.assertEquals(4, molecules.size());
    }

    @Test
    public void updateByMorphiaUpdate() throws Exception {

        datastore.save(Arrays.asList(
                Molecule.info("Water", "H2O2"),
                Molecule.info("Hydrogen Peroxide", "H2O2"),
                Molecule.info("Carbon Monoxide", "CO"),
                Molecule.info("Carbon Dioxide", "CO2")
        ));

        Query<Molecule> waterFilter = datastore.createQuery(Molecule.class)
                .filter("name", "Water");

        UpdateOperations<Molecule> updateOperations =
                datastore.createUpdateOperations(Molecule.class).set("formula", "H2O");

        datastore.update(waterFilter, updateOperations);

        List<Molecule> molecules = datastore.createQuery(Molecule.class).asList();
        molecules.forEach(System.out::println);
        Assert.assertEquals(4, molecules.size());
    }

    @Test
    public void find() throws Exception {

        datastore.save(Arrays.asList(
                Molecule.details("Water", "H2O2", Arrays.asList("H", "O"), 18),
                Molecule.details("Hydrogen Peroxide", "H2O2", Arrays.asList("O"), 34),
                Molecule.details("Carbon Monoxide", "CO", Arrays.asList("C", "O"), 28),
                Molecule.details("Carbon Dioxide", "CO2", Arrays.asList("C", "O"), 44)
        ));

        Query<Molecule> query = datastore.createQuery(Molecule.class);
        query.or(
                query.criteria("mass").greaterThan(30),
                query.criteria("mass").lessThan(20)
        );

        List<Molecule> foundMolecules = query.asList();
        foundMolecules.forEach(System.out::println);
        Assert.assertEquals(3, foundMolecules.size());
    }

    @Test
    public void aggregate() throws Exception {

        AggregationOptions options = AggregationOptions.builder().build();
        datastore.save(Arrays.asList(
                Molecule.details("Water", "H2O2", Arrays.asList("H", "O"), 18),
                Molecule.details("Hydrogen Peroxide", "H2O2", Arrays.asList("O"), 34),
                Molecule.details("Carbon Monoxide", "CO", Arrays.asList("C", "O"), 28),
                Molecule.details("Carbon Dioxide", "CO2", Arrays.asList("C", "O"), 44)
        ));

        Iterator<AverageMassReport> iterator = datastore.createAggregation(Molecule.class)
                .unwind("atoms")
                .group("atoms", grouping("averageMass", average("mass")))
                .out(AverageMassReport.class, options);


        List<AverageMassReport> aggregatedMasses = new ArrayList<>();
        iterator.forEachRemaining(aggregatedMasses::add);

        aggregatedMasses.forEach(System.out::println);
        Assert.assertEquals(3, aggregatedMasses.size());
    }

    @Test
    public void aggregateSomeMore() throws Exception {

        AggregationOptions options = AggregationOptions.builder().build();
        datastore.save(Arrays.asList(
                Molecule.details("Water", "H2O2", Arrays.asList("H", "O"), 18),
                Molecule.details("Hydrogen Peroxide", "H2O2", Arrays.asList("O"), 34),
                Molecule.details("Carbon Monoxide", "CO", Arrays.asList("C", "O"), 28),
                Molecule.details("Carbon Dioxide", "CO2", Arrays.asList("C", "O"), 44)
        ));

        Iterator<AverageMassReport> iterator = datastore.createAggregation(Molecule.class)
                .match(datastore.createQuery(Molecule.class).filter("name !=", "Hydrogen Peroxide"))
                .unwind("atoms")
                .group("atoms",
                        grouping("averageMass", average("mass")),
                        grouping("count", accumulator("$sum", 1))
                )
                .out(AverageMassReport.class, options);

        List<AverageMassReport> aggregatedMasses = new ArrayList<>();
        iterator.forEachRemaining(aggregatedMasses::add);

        aggregatedMasses.forEach(System.out::println);
        Assert.assertEquals(3, aggregatedMasses.size());
    }

    private static class AverageMassReport {
        private String _id;
        private Integer averageMass;
        private Integer count;

        @Override
        public String toString() {
            return "AverageMassReport{" +
                    "_id='" + _id + '\'' +
                    ", averageMass=" + averageMass +
                    ", count=" + count +
                    '}';
        }
    }
}
