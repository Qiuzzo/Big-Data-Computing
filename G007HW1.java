import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.io.IOException;
import java.util.*;

public class G007HW1{
    public static void main(String[] args) throws IOException{


        if (args.length != 3) {
            throw new IllegalArgumentException("USAGE: num_partitions num_runs file_path");
        }

        SparkConf conf = new SparkConf(true).setAppName("G007HW1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        /* Variables used: */
        int C = Integer.parseInt(args[0]); // Read number of colors/partitions to use
        int R = Integer.parseInt(args[1]); // Read number of executions of function "MR_ApproxTCwithNodeColors(...)"
        Long[] triangNumList = new Long[R]; //Array that contains the num of Triangles counts at each execution of function "MR_ApproxTCwithNodeColors(...)"
        Long[] runTimeList = new Long[R];//Array that contains the runTIme of each execution of function "MR_ApproxTCwithNodeColors(...)"
        long start_time=0L; //measure of the starting time execution
        long end_time=0L; //measure of the ending time execution
        long final_exe_time=0L; //final execution time
        long final_exe_time_app=0L; //final execution time
        long median=0L; //median of the R estimates returned by "MR_ApproxTCwithNodeColors(...)"


        // Read input file
        JavaRDD<String> rawData = sc.textFile(args[2]);

        // Convert rawData into an RDD of edges
        JavaPairRDD<Integer, Integer> edges = rawData.flatMapToPair(
                (document) -> {

                    String[] tokens = document.split(",");
                    HashMap<Integer, Integer> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

                    String curr = null;
                    for(String token : tokens){
                        if (curr != null && !curr.equals(token)){
                            counts.put(Integer.parseInt(curr), Integer.parseInt(token));
                        }
                        curr = token;
                    }

                    for (Map.Entry<Integer, Integer> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }

                    return pairs.iterator();
                });


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // ALGORITHM 1 - EXECUTION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //R execution of function "MR_ApproxTCwithNodeColors(edges, C)"
        for (int j = 0; j < R; j++) {
            start_time = System.currentTimeMillis();
            triangNumList[j] = MR_ApproxTCwithNodeColors(edges, C);
            end_time = System.currentTimeMillis();
            runTimeList[j] = end_time-start_time;
        }

        Arrays.sort(triangNumList);

        /*Median computation: */
        if (triangNumList.length % 2 == 0)
            median = (triangNumList[triangNumList.length/2] + triangNumList[triangNumList.length/2 - 1])/2;
        else
            median = triangNumList[triangNumList.length/2];

        /*Computation of final execution time: */
        long timeSum = 0L;
        for(Long e : runTimeList){
            timeSum += e;
        }
        final_exe_time=timeSum/runTimeList.length;


        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // ALGORITHM 2 - EXECUTION
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        //Spark repartition
        JavaPairRDD<Integer, Integer> e1 = edges.repartition(C).cache();
        start_time = System.currentTimeMillis();
        long trianglesWithSparkPartitions = MR_ApproxTCwithNodeColors(edges, C);
        end_time = System.currentTimeMillis();
        final_exe_time_app=end_time - start_time;



        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        // FINAL OUTPUT
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
        System.out.println("\nFINAL OUTPUT:");

        System.out.println("Dataset = " + args[2]);
        System.out.println("Number of Edges = " + edges.count());
        System.out.println("Number of Colors = " + C);
        System.out.println("Number of Repetitions = " + R);
        System.out.println("---------------------------------------------------------");
        System.out.println("Approximation through node coloring:");
        System.out.println("- Number of triangles (median over 1 runs) = " + median);
        System.out.println("- Running time (average over 1 runs) = " + final_exe_time);
        System.out.println("---------------------------------------------------------");
        System.out.println("Approximation through Spark partitions:");
        System.out.println("- Number of triangles = " + trianglesWithSparkPartitions);
        System.out.println("- Running time = " + final_exe_time_app);
        System.out.println("---------------------------------------------------------");

    }

    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // 2 ROUND - ALGORITHM 1
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static Long  MR_ApproxTCwithNodeColors(JavaPairRDD<Integer, Integer> edges, int C){

        Random rnd = new Random();
        int p = 8191;
        int a = rnd.nextInt((p - 1)) + 1;
        int b = rnd.nextInt(p);

        JavaPairRDD<Integer, Long> edgeSet = edges.flatMapToPair( // <-- MAP PHASE (R1)
                (pair) -> {

                    ArrayList<Tuple2<Integer, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();

                    // Color assignment
                    int hC1 = ((a * pair._1() + b) % p) % C;
                    int hC2 = ((a * pair._2() + b) % p) % C;

                    // Check if the color of two nodes are equal
                    if (hC1 == hC2) {
                        pairs.add(new Tuple2<>(hC1, pair));
                    }

                    return pairs.iterator();
                })
                .groupByKey()
                .flatMapToPair((element) -> { // -> REDUCE PHASE (1)
                    ArrayList<Tuple2<Integer, Integer>> couple = new ArrayList<>();
                    for (Tuple2<Integer, Integer> e : element._2()) {
                        couple.add(e);
                    }
                    ArrayList<Tuple2<Integer, Long>> sTriangles = new ArrayList<>();
                    sTriangles.add(new Tuple2<>(0, CountTriangles(couple)));
                    return sTriangles.iterator();
                })
                .groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return Double.valueOf(Math.pow(C, 2) * sum).longValue();
                });

        long numTriangles = edgeSet.map(Tuple2::_2).reduce((x, y) -> y); // <-- REDUCE PHASE 2;

        return numTriangles;
    }
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    // 2 ROUND - ALGORITHM 2
    // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    public static Long MR_ApproxTCwithSparkPartitions(JavaPairRDD<Integer, Integer> edges) {

        int nP = edges.getNumPartitions();
        JavaPairRDD<Integer, Long> edgeSet = edges
                .mapPartitionsToPair((element) -> { // -> REDUCE PHASE (1)
                    ArrayList<Tuple2<Integer, Integer>> couples = new ArrayList<>();

                    while(element.hasNext()){
                        Tuple2<Integer, Integer> couple = element.next();
                        couples.add(new Tuple2<>(couple._1(), couple._2()));
                    }

                    ArrayList<Tuple2<Integer, Long>> sTriangles = new ArrayList<>();
                    sTriangles.add(new Tuple2<>(0, CountTriangles(couples)));
                    return sTriangles.iterator();
                })
                .groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it) {
                        sum += c;
                    }
                    return (long)(Math.pow(nP, 2) * sum);
                });

        long numTriangles = edgeSet.map(Tuple2::_2).reduce((x, y) -> y); // <-- REDUCE PHASE 2;

        return numTriangles;
    }

    public static Long CountTriangles(ArrayList<Tuple2<Integer, Integer>> edgeSet) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
            if (uAdj == null) {uAdj = new HashMap<>();}
            uAdj.put(v,true);
            adjacencyLists.put(u,uAdj);
            if (vAdj == null) {vAdj = new HashMap<>();}
            vAdj.put(u,true);
            adjacencyLists.put(v,vAdj);
        }
        Long numTriangles = 0L;
        for (int u : adjacencyLists.keySet()) {
            HashMap<Integer,Boolean> uAdj = adjacencyLists.get(u);
            for (int v : uAdj.keySet()) {
                if (v>u) {
                    HashMap<Integer,Boolean> vAdj = adjacencyLists.get(v);
                    for (int w : vAdj.keySet()) {
                        if (w>v && (uAdj.get(w)!=null)) numTriangles++;
                    }
                }
            }
        }
        return numTriangles;
    }
}