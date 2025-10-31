package project;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.to_json;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class AccountStreamFullApp {
	public static void main(String[] args) throws StreamingQueryException {
		SparkSession spark=SparkSession.builder().appName("AccountStreamFullApp").master("local[*]").getOrCreate();
		spark.sparkContext().setLogLevel("Warn");

		StructType accountSchema=new StructType()
				.add("accountNumber",DataTypes.LongType)
				.add("customerId",DataTypes.LongType)
				.add("accountType",DataTypes.StringType)
				.add("branch",DataTypes.StringType);

		try {
		Dataset<Row> fileStream=spark.readStream().option("header","true").schema(accountSchema).json("C:/capstone");

		Dataset<Row> kafkaFormatted=fileStream.select(to_json(struct(col("accountNumber"),col("customerId"),
				col("accountType"),col("branch"))).alias("value"),col("accountType").alias("key"));

		StreamingQuery fileToKafka=kafkaFormatted.writeStream().format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("topic", "accounts-topic")
				.option("checkpointLocation", "c:/capstone/checkpoints/file")
				.outputMode("append")
				.start();
		
		Dataset<Row> kafkaStream=spark.readStream()
				.format("kafka")
				.option("kafka.bootstrap.servers", "localhost:9092")
				.option("subscribe", "accounts-topic")
				.option("startingOffsets","latest")
				.load();
		
		Dataset<Row> parsed=kafkaStream.selectExpr("CAST(value AS STRING) as json_str")
				.select(from_json(col("json_str"),accountSchema).alias("data")).select("data.*");

		Dataset<Row> caStream=parsed.filter(col("accountType").equalTo("CA"))
				.select("accountNumber","customerId","branch");
		
		caStream.writeStream().format("csv")
		.option("path", "c:/capstone/ca")
		.option("checkpointLocation", "c:/capstone/checkpoints/ca")
		.outputMode("append")
		.start();
		
		

		Dataset<Row> sbStream=parsed.filter(col("accountType").equalTo("SB"))
				.select("accountNumber","customerId","branch");
		
		sbStream.writeStream().format("json")
		.option("path", "c:/capstone/sb")
		.option("checkpointLocation", "c:/capstone/checkpoints/sb")
		.outputMode("append")
		.start();
		

		Dataset<Row> rdStream=parsed.filter(col("accountType").equalTo("RD"))
				.select("accountNumber","customerId","branch");
		
		rdStream.writeStream().format("avro")
		.option("path", "c:/capstone/rd")
		.option("checkpointLocation", "c:/capstone/checkpoints/rd")
		.outputMode("append")
		.start();

		Dataset<Row> loanStream=parsed.filter(col("accountType").equalTo("LOAN"))
				.select("accountNumber","customerId","branch");
		
		loanStream.writeStream().format("parquet")
		.option("path", "c:/capstone/loan")
		.option("checkpointLocation", "c:/capstone/checkpoints/loan")
		.outputMode("append")
		.start();

		

		StreamingQuery aggQuery=parsed.groupBy(col("branch"),col("accountType")).count()
				.withColumnRenamed("count","number_of_accounts")
		.writeStream()
		.foreachBatch((batchDF,batchId)->{
			batchDF.write()
			.format("jdbc")
			.option("url","jdbc:mysql://localhost:3306/trainingdb")
			.option("dbtable", "analytics_table")
			.option("user", "root")
			.option("password","Password@1")
			.mode(SaveMode.Append).save();
		})
		.option("checkpointLocation","C:/capstone/checkpoints/agg")
		.outputMode("update")
		.start();
		
		System.out.println("Streaming Started.");
		Thread.sleep(10*60*1000);
		
		spark.streams().awaitAnyTermination();
		
		}catch(Exception e) {
			System.out.println("Failed" + e.getMessage());
			e.printStackTrace();
		}
		finally {
			spark.close();
		}
		
	}
}
