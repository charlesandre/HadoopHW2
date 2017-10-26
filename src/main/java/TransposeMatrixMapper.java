import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TransposeMatrixMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    public void map(LongWritable key, Text entry, Context context
    ) throws IOException, InterruptedException {

        //Get the entry to iterate on it
        StringTokenizer input = new StringTokenizer(entry.toString());


        //Variables we'll need in the loop
        String position;
        String value;
        int columnIndex = 0;

        //Go through the lines
        while (input.hasMoreTokens()) {

            //Separate with the ',' and get the value/content of the cell
            value = input.nextToken(",");

            //Get the key which is the number of the line and put it in a string with the value  as "line,value"
            position = Long.toString(key.get()) + "," + value;

            //output in the context the number of the column and the string with "line,value"
            context.write(new IntWritable(columnIndex), new Text(position));

            //Increase the column index as we move forward
            columnIndex++;
        }
    }
}