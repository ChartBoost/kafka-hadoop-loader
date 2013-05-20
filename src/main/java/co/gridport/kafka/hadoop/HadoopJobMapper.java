package co.gridport.kafka.hadoop;

import java.io.IOException;

import org.apache.commons.lang.NotImplementedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class HadoopJobMapper extends Mapper<LongWritable, BytesWritable, Text, Text> {

	private static JsonFactory jsonFactory = new JsonFactory();
	private static ObjectMapper objectMapper = new ObjectMapper();
	private static DateTimeFormatter fmt = DateTimeFormat.forPattern("yyyyMMdd/HH");

	@Override
	public void map(LongWritable key, BytesWritable value, Context context) throws IOException {
		try {
			Text outDateKey = getKey(key, value, context.getConfiguration());
			Text outValue = new Text();
			outValue.set(value.getBytes(), 0, value.getLength());
			context.write(outDateKey, outValue);
			context.getCounter("HadoopJobMapper", "Processed").increment(1);
		} catch (Throwable e) {
			context.getCounter("HadoopJobMapper", e.getClass().getCanonicalName()).increment(1);
			e.printStackTrace();
		}
	}

	public Text getKey(LongWritable key, BytesWritable value, Configuration conf) throws IOException {
		String inputFormat = conf.get("input.format");
		if (inputFormat.equals("json")) {
			Text outDateKey = new Text();

			JsonParser jp = jsonFactory.createJsonParser(value.getBytes());
			JsonNode root = objectMapper.readTree(jp);
			JsonNode time = root.path("time");
			JsonNode date_created = root.path("date_created");

			if (!time.isMissingNode()) {
				long ts = time.getLongValue() * 1000;
				DateTime dt = new DateTime(ts);
				outDateKey.set(fmt.print(dt));
			} else if (!date_created.isMissingNode()) {
				long ts = date_created.getLongValue() * 1000;
				DateTime dt = new DateTime(ts);
				outDateKey.set(fmt.print(dt));
			} else {
				DateTime dt = new DateTime();
				outDateKey.set(fmt.print(dt));
			}
			return outDateKey;
		} else if (inputFormat.equals("protobuf")) {
			throw new NotImplementedException("Protobuf input disabled");
		} else if (inputFormat.equals("avro")) {
			throw new NotImplementedException("Avro input format not implemented");
		} else {
			throw new IOException("Invalid mapper input.format");
		}
	}

}
