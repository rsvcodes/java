import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class LogMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        // Buscar patrones de [INFO], [SEVERE], [WARN] en la línea
        if (line.contains("[INFO]")) {
            word.set("INFO");
            context.write(word, one);
        } else if (line.contains("[SEVERE]")) {
            word.set("SEVERE");
            context.write(word, one);
        } else if (line.contains("[WARN]")) {
            word.set("WARN");
            context.write(word, one);
        }
    }
}