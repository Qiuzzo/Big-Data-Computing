import com.codahale.metrics.Histogram;
import org.apache.hadoop.util.hash.Hash;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Interval;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import shapeless.Tuple;

import java.time.temporal.Temporal;
import java.util.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class G007HW3 {

    public static final int THRESHOLD = 10000000;

    public static void main(String[] args) throws Exception {
        if (args.length != 6) {
            throw new IllegalArgumentException("USAGE: rows, columns, lEndpoint, rEndpoint, topFreqItems, port");
        }

        int D = Integer.parseInt(args[0]);
        int W = Integer.parseInt(args[1]);
        int left = Integer.parseInt(args[2]);
        int right = Integer.parseInt(args[3]);
        int K = Integer.parseInt(args[4]);
        int portExp = Integer.parseInt(args[5]);



        // IMPORTANT: when running locally, it is *fundamental* that the
        // `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
        // there will be no processor running the streaming computation and your
        // code will crash with an out of memory (because the input keeps accumulating).
        SparkConf conf = new SparkConf(true)
                .setMaster("local[*]") // remove this line if running on the cluster
                .setAppName("DistinctExample");

        // Here, with the duration you can control how large to make your batches.
        // Beware that the data generator we are using is very fast, so the suggestion
        // is to use batches of less than a second, otherwise you might exhaust the
        // JVM memory.
        JavaStreamingContext sc = new JavaStreamingContext(conf, Durations.milliseconds(100));
        sc.sparkContext().setLogLevel("ERROR");

        // TECHNICAL DETAIL:
        // The streaming spark context and our code and the tasks that are spawned all
        // work concurrently. To ensure a clean shut down we use this semaphore.
        // The main thread will first acquire the only permit available and then try
        // to acquire another one right after spinning up the streaming computation.
        // The second tentative at acquiring the semaphore will make the main thread
        // wait on the call. Then, in the `foreachRDD` call, when the stopping condition
        // is met we release the semaphore, basically giving "green light" to the main
        // thread to shut down the computation.
        // We cannot call `sc.stop()` directly in `foreachRDD` because it might lead
        // to deadlocks.
        Semaphore stoppingSemaphore = new Semaphore(1);
        stoppingSemaphore.acquire();

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // INPUT READING
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println("Receiving data from port = " + portExp);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        long[] streamLength = new long[1]; // Stream length (an array to be passed by reference)
        streamLength[0]=0L;
        HashMap<Long, Long> histogram = new HashMap<>(); // Hash Table for the distinct elements
        HashMap<Long, ArrayList<Tuple2<Long, Long>>> test = new HashMap<>();
        HashMap<Long, Long> count_items = new HashMap<>();
        long[][] counters = new long[D][W];


        for (int i = 0; i < D; i++){
            for (int j = 0; j < W; j++){
                counters[i][j] = 0;
            }
        }
        HashMap<Long, ArrayList<Tuple2<Long, Long>>> data = new HashMap<>();

        // CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
        sc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevels.MEMORY_AND_DISK)
                // For each batch, to the following.
                // BEWARE: the `foreachRDD` method has "at least once semantics", meaning
                // that the same data might be processed multiple times in case of failure.
                .foreachRDD((batch, time) -> {
                    // this is working on the batch at time `time`.
                    if (streamLength[0] < THRESHOLD) {
                        long batchSize = batch.count();
                        streamLength[0] += batchSize;
                        // Extract the distinct items from the batch
                        Map<Long, Long> batchItems = batch
                                .mapToPair(s -> new Tuple2<>(Long.parseLong(s), 1L))
                                .groupByKey()
                                .mapValues((it) -> {
                                    long sum = 0;
                                    for (long c : it) {
                                        sum += c;
                                    }
                                    return (long)(sum);
                                }).collectAsMap();

                        //REAL FREQUENCIES: Update the streaming_R state + compute real frequencies
                        //and APPROXIMATE FREQUENCIES
                        for (Map.Entry<Long, Long> pair : batchItems.entrySet()) {
                            ArrayList<Tuple2<Long, Long>> hash_functions = new ArrayList<>();   //contains the 2 hash_functions
                            if ((pair.getKey()>=left) && (pair.getKey() <= right)) {
                                long count=pair.getValue();  //number  of item of type key into the current batch
                                if (!histogram.containsKey(pair.getKey())) {
                                    for (int j = 0; j < D; j++) {
                                        long h_j = hashSketch(pair.getKey(),W);
                                        long g_j = signHash(pair.getKey(), W);
                                        hash_functions.add(new Tuple2<>(h_j, g_j));
                                    }
                                    test.put(pair.getKey(), hash_functions);//insert the new couples of hash functions into support data structure
                                    histogram.put(pair.getKey(), pair.getValue());

                                } else {
                                    histogram.put(pair.getKey(),  histogram.get(pair.getKey()) + pair.getValue());
                                }

                                ArrayList<Tuple2<Long, Long>> temp = new ArrayList<>(); //Support data structure
                                for (int i = 0; i < count; i++){
                                    for (int j = 0; j < D; j++) {
                                        Tuple2<Long, Long> t=test.get(pair.getKey()).get(j);
                                        counters[j][t._1().intValue()] += t._2();
                                        temp.add(new Tuple2<>(t._1(), t._2()));
                                    }
                                }
                            }
                        }
                        if (streamLength[0] >= THRESHOLD) {
                            stoppingSemaphore.release();
                        }
                    }
                });

        // MANAGING STREAMING SPARK CONTEXT
        System.out.println("Starting streaming engine");
        sc.start();
        System.out.println("Waiting for shutdown condition");
        stoppingSemaphore.acquire();
        System.out.println("Stopping the streaming engine");
        // NOTE: You will see some data being processed even after the
        // shutdown command has been issued: This is because we are asking
        // to stop "gracefully", meaning that any outstanding work
        // will be done.
        sc.stop(false, true);
        System.out.println("Streaming engine stopped");

        // COMPUTE AND PRINT FINAL STATISTICS

        // &&&&&&&&&&&&&&&&&&&
        // TRUE SECOND MOMENT
        // &&&&&&&&&&&&&&&&&&&
        double F2_true = 0;
        double count_elem_stream_R=0;
        for (Long value : histogram.values()){

            F2_true += Math.pow(value, 2);
            count_elem_stream_R+=value;
        }

        // Normalization of the true second moment
        F2_true = F2_true / Math.pow(count_elem_stream_R, 2);

        // &&&&&&&&&&&&&&&&&&&&&&&&&&
        // APPROXIMATE SECOND MOMENT
        // &&&&&&&&&&&&&&&&&&&&&&&&&&

        double[] F2_approx = new double[D];
        double median = 0;
        for (int i = 0; i < D; i++){
            for (int j = 0; j < W; j++){
                F2_approx[i] += Math.pow(counters[i][j], 2);
            }
        }

        if (F2_approx.length % 2 == 0)
            median = (F2_approx[F2_approx.length / 2] + F2_approx[F2_approx.length / 2 - 1]) / 2;
        else
            median = F2_approx[F2_approx.length / 2];

        median = median / Math.pow(count_elem_stream_R, 2);

        System.out.println("D = " + D + " W = " + W + " [" + left + "," + right + "]" + " K = " + K + " Port = " + portExp);
        System.out.println("Total number of items = " + streamLength[0]);
        System.out.println("Total number of items in " + "[" + left + "," + right + "]" + " = " + count_elem_stream_R);
        System.out.println("Number of distinct items = " + histogram.size());

        // &&&&&&&&&&&&&&&&&&&&&&&
        // AVERAGE RELATIVE ERROR
        // &&&&&&&&&&&&&&&&&&&&&&&

        //Sorting true frequencies
        Collection<Long> trueFreq = histogram.values();
        ArrayList<Long> sorted_trueFreq = new ArrayList<>(trueFreq);
        sorted_trueFreq.sort(Collections.reverseOrder());

        //The K-th largest frequency of the items of SigmaR
        Long k_th_freq = sorted_trueFreq.get(K - 1);
        System.out.println(k_th_freq);

        ArrayList<Tuple2<Long, Double>> sorted_approxFreq = new ArrayList<>();

        ArrayList<Double> relative_errors = new ArrayList<>();  //it contains the relative error of each K elements
        for (Map.Entry<Long,Long> e : histogram.entrySet()) {
            if (e.getValue() >= k_th_freq) {
                ArrayList<Tuple2<Long, Long>> f = test.get(e.getKey());
                ArrayList<Double> temp_freq = new ArrayList<>();

                //computation of estimated frequencies
                for(int i = 0; i < D; i++) {
                    Tuple2<Long, Long> h = f.get(i);
                    temp_freq.add((double)(counters[i][h._1().intValue()] * h._2()));
                }

                double freq_median = 0;
                Collections.sort(temp_freq);
                if (temp_freq.size() % 2 == 0)
                    freq_median = (temp_freq.get(temp_freq.size() / 2) + temp_freq.get(temp_freq.size() / 2 - 1)) / 2;
                else
                    freq_median = temp_freq.get(temp_freq.size() / 2);

                if(K <= 20){
                    System.out.println("Item " + e.getKey() + " Freq = " + e.getValue() + " Est. Freq = " + freq_median);
                }

                relative_errors.add(Math.abs((e.getValue() - freq_median))/e.getValue());
            }
        }

        double sum = 0;
        for(double c : relative_errors){
            sum += c;
        }
        double average = sum / (relative_errors.size());

        System.out.println("Avg err for top " + K + " = " + average);
        System.out.println("F2 " + F2_true + " F2 Estimate " + median);
    }

    //hash functions
    static long hashSketch(long value, int W){
        Random rnd = new Random();
        int p = 8191;
        int a = rnd.nextInt((p - 1)) + 1;
        int b = rnd.nextInt(p);
        return ((a * value + b) % p) % W;
    }

    static long signHash(long value, int W){
        Random rnd = new Random();
        int p = 8191;
        int a = rnd.nextInt((p - 1)) + 1;
        int b = rnd.nextInt(p);
        if (((a * value + b) % p) % W % 2 == 0) {
            return 1;
        }
        else {
            return -1;
        }
    }
}
