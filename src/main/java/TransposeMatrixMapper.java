
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

public class TransposeMatrixMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
    public void map(LongWritable key, Text entry, Context context
    ) throws IOException, InterruptedException {
        int columnIndex = 0;
        String value;
        String position;
        //Get the entry to iterate on it
        StringTokenizer itr = new StringTokenizer(entry.toString());
        //Go through the lines
        while (itr.hasMoreTokens()) {
            //Separate with the ',' and get the value/content of the cell
            value = itr.nextToken(",");
            //Get the key which is the number of the line and put it in a string with the value  as "line,value"
            position = Long.toString(key.get()) + "," + value;
            System.out.println(position);
            //output in the context the number of the column and the string with "line,value"
            context.write(new IntWritable(columnIndex), new Text(position));
            //Increase the column index as we move forward
            columnIndex++;
        }
    }
}