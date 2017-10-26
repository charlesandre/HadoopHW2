import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;


import java.io.FileWriter;
import java.io.IOException;

public class TransposeMatrixReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> entry, Context context
    ) throws IOException, InterruptedException {
        Text output = new Text();
        String row = "";

            //We create an arrayList where we put all the entry as strings.
        ArrayList<String> orderedLine = new ArrayList<String>();

        for (Text ent : entry) {
            System.out.println(ent.toString());
            orderedLine.add(ent.toString());
        }
        // We sort it
        Collections.sort(orderedLine);
        // We then go through all the strings
        for (String t : orderedLine) {
            if(row != ""){
                row += ",";         //We add a "," between every value.
            }
            row += t.split(",")[1]; //We take only the part after the "," which is the value
        }
        //We put each row in a Text
        output.set(row);
        // We output the key which is the number of the line, and the values, separated by coma.
        context.write(key, output);
    }
}