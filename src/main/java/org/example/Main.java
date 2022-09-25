package org.example;

import java.awt.geom.Point2D;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
public class Main
{

    public static void main(String[] args) throws Exception
    {

// Create a Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("wordCount").setMaster("local"));

        // Load our input data
//        JavaRDD<String> lines = sc.textFile(args[0]);

        JavaStreamingContext ssc = new JavaStreamingContext(sc, new Duration(35000));



        // Calculate word count

//
//        JavaPairRDD<String, Integer> threshold = counts.filter(x -> Integer.valueOf(args[2]) >= x._2()  )
//                .flatMap(x -> Arrays.asList(StringUtils.repeat(x._1, x._2).split("")))
//                .mapToPair(z -> new Tuple2<String, Integer>(z, 1))
//                .reduceByKey((x, y) -> x + y);

        Set<String> topics = Collections.singleton("myTopic");
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        JavaPairDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics)
                .mapToPair(a -> {
                    if(a._2.split(" ")[3].equals("driver")){
                    return new Tuple2<String, String>(a._2.split(" ")[3],a._2);
                }
                return new Tuple2<String,String>(a._2.split(" ")[0],a._2);
                })

                .reduceByKey(
                (a,b) -> a+" "+b
        );


        directKafkaStream.foreachRDD(rdd -> {
            if(rdd.count() > 0){
                System.out.println("--- New RDD with " + rdd.partitions().size()
                        + " partitions and " + rdd.count() + " records");
                AtomicReference<String> driversList = new AtomicReference<>("A");

                JavaPairRDD<String, String> driverRdd = rdd.filter(a -> a._1.equals("driver"));
                if(driverRdd.count() == 0) {
                    System.out.println("driver not found for user, so the app will keep publishing messages to kafka");

                    return;
                }
                String drivers = driverRdd.collect().get(0)._2;

                JavaPairRDD<String, String> counts = rdd.filter(a -> !a._1.equals("driver")).mapToPair(a -> {

                        return new Tuple2<>(a._2,getNearestDriver(drivers,Double.valueOf(a._2.split(" ")[1]),Double.valueOf(a._2.split(" ")[2])));
                });
                counts.saveAsTextFile("file");
                saveToHbase("usr",counts);
            }
        });

        ssc.start();
        ssc.awaitTermination();
        sc.close();
    }

    public static void saveToHbase(String tableName, JavaPairRDD<String, String> rdd) {
        rdd.foreachPartition(iterator -> {
                    try (Connection connection = ConnectionFactory.createConnection(HBaseConfiguration.create());
                         //option 1.1 is to use Table table = connection.getTable(TableName.valueOf(tableName));
                         BufferedMutator mutator = connection.getBufferedMutator(TableName.valueOf(tableName))) {
                        while (iterator.hasNext()) {
                            Tuple2<String,String> record = iterator.next();
                            Put put = new Put(Bytes.toBytes(record._1));
                            put.addColumn(Bytes.toBytes("usr_data"), Bytes.toBytes("driver"), Bytes.toBytes(record._2));
                            mutator.mutate(put);
                            //table.put(put);
                        }
                    }
                }
        );
    }
    public static String getNearestDriver(String drivers, double x, double y){
        String driverId = "0";
        double minDistance = Integer.MAX_VALUE;
        for(String driver:drivers.split(",")){
            double distance = calculateDistanceBetweenPointsWithPoint2D(Integer.valueOf(driver.split(" ")[1]), Integer.valueOf(driver.split(" ")[2]),x,y);
            if(distance < minDistance) {
                driverId = driver.split(" ")[0];
                minDistance = distance;
            }
        }
        return driverId;

    }

    public static double calculateDistanceBetweenPointsWithPoint2D(
            double x1,
            double y1,
            double x2,
            double y2) {

        return Point2D.distance(x1, y1, x2, y2);
    }


}