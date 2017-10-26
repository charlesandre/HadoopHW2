import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.*;
import java.io.IOException;

public class TransposeMatrixReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    public void reduce(IntWritable key, Iterable<Text> entry, Context context
    ) throws IOException, InterruptedException {

        //We create an arrayList where we put all the entry as strings.
        ArrayList<String> Lines = new ArrayList<String>();

        for (Text ent : entry) {
            Lines.add(ent.toString());
        }
        // We sort it
        Collections.sort(Lines);


        // We then go through all the strings
        String row = "";

        for (String t : Lines) {
            //We add a "," between every value.
            if(row != ""){
                row += ",";
            }
            //We take only the part after the "," which is the value
            row += t.split(",")[1];
        }
        //We put each row in a Text
        Text output = new Text();
        output.set(row);

        // We output the key which is the number of the line, and the values, separated by coma.
        context.write(key, output);
    }
}