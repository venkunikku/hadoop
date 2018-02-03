package com.home.hadoop.wordcounts;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.StringTokenizer;

public class Testclass {

    public static void main(String[] args) {
        try {
            BufferedReader read = new BufferedReader(new FileReader(new File("C:\\Users\\vburag0\\IdeaProjects\\HomeHadoopDotCom\\src\\main\\java\\com\\home\\hadoop\\wordcounts\\input.txt")));
            String line;
            int i= 0;
            while ((line = read.readLine())!=null){
                    System.out.println(i++);
                    System.out.println(line);
                StringTokenizer token = new StringTokenizer(line);
                while (token.hasMoreElements()){
                    System.out.println(token.nextToken());
                }
            }

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
