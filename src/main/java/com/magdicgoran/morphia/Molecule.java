package com.magdicgoran.morphia;

import org.bson.types.ObjectId;
import org.mongodb.morphia.annotations.Entity;
import org.mongodb.morphia.annotations.Id;

import java.util.Collections;
import java.util.List;

@Entity("molecules")
public class Molecule {

    @Id
    private ObjectId id;
    private String name;
    private String formula;
    private List<String> atoms;
    private Integer mass;

    private Molecule() {
    }

    public Molecule(String name, String formula, List<String> atoms, Integer mass) {
        this.name = name;
        this.formula = formula;
        this.atoms = atoms;
        this.mass = mass;
    }

    public static Molecule info(String name, String formula) {
        return new Molecule(name, formula, Collections.emptyList(), null);
    }

    public static Molecule details(String name, String formula, List<String> atoms, Integer mass) {
        return new Molecule(name, formula, atoms, mass);
    }

    public ObjectId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getFormula() {
        return formula;
    }

    public List<String> getAtoms() {
        return atoms;
    }

    public Integer getMass() {
        return mass;
    }

    public void setFormula(String formula) {
        this.formula = formula;
    }

    @Override
    public String toString() {
        return "Molecule{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", formula='" + formula + '\'' +
                ", atoms=" + atoms +
                ", mass=" + mass +
                '}';
    }
}
