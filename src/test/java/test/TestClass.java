package test;

import java.util.TreeSet;

public class TestClass {

    public static void main(String[] args) {
        TreeSet<String> treeSet = new TreeSet<String>();
        treeSet.add("Apple");
        treeSet.add("Grape");
        treeSet.add("Apple");
        treeSet.add("Sample");
        treeSet.add("apple");

        for (String s: treeSet){
            System.out.println("Values: "+ s);
        }

        if (treeSet.size() > 2) {
            treeSet.remove(treeSet.last());
        }


        for (String s: treeSet){
            System.out.println("After Values: "+ s);
        }

    }
}
