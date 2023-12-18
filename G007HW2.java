import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;
import java.io.IOException;
import java.util.*;


public class G007HW2 {

    public static void main(String[] args) throws IOException {


        if (args.length != 4) {
            throw new IllegalArgumentException("USAGE: num_partitions num_runs flag file_path");
        }

        SparkConf conf = new SparkConf(true).setAppName("G007HW2");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

        conf.set("spark.locality.wait", "0s");

        /* Variables used: */
        int C = Integer.parseInt(args[0]); // Read number of colors/partitions to use
        int R = Integer.parseInt(args[1]); // Read number of executions of function "MR_ApproxTCwithNodeColors(...)"
        int F = Integer.parseInt(args[2]); //
        if (F != 0 && F != 1) {
            throw new IllegalArgumentException("USAGE: F must be 0 or 1");
        }
        String filePath = args[3];
        Long[] triangNumList = new Long[R]; //Array that contains the num of Triangles counts at each execution of function "MR_ApproxTCwithNodeColors(...)"
        Long[] runTimeList = new Long[R];//Array that contains the runTIme of each execution of function "MR_ApproxTCwithNodeColors(...)"
        long start_time = 0L; //measure of the starting time execution
        long end_time = 0L; //measure of the ending time execution
        long final_exe_time = 0L; //final execution time
        long median = 0L; //median of the R estimates returned by "MR_ApproxTCwithNodeColors(...)"


        // Read input file
        JavaRDD<String> rawData = sc.textFile(filePath);

        // Convert rawData into an RDD of edges
        JavaPairRDD<Integer, Integer> edges = rawData.flatMapToPair(
                (document) -> {

                    String[] tokens = document.split(",");
                    HashMap<Integer, Integer> counts = new HashMap<>();
                    ArrayList<Tuple2<Integer, Integer>> pairs = new ArrayList<>();

                    String curr = null;
                    for (String token : tokens) {
                        if (curr != null && !curr.equals(token)) {
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
        // FINAL OUTPUT
        // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

        System.out.println("\nFINAL OUTPUT:");
        System.out.println("Dataset = " + filePath);
        System.out.println("Number of Edges = " + edges.count());
        System.out.println("Number of Colors = " + C);
        System.out.println("Number of Repetitions = " + R);
        System.out.println("---------------------------------------------------------");

        if (F == 0) {

            JavaPairRDD<Integer, Integer> edgesSparkRDD = edges.repartition(32).cache();

            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
            // ALGORITHM 1 - EXECUTION
            // &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

            // R execution of function "MR_ApproxTCwithNodeColors(edgesSparkRDD, C)"
            for (int j = 0; j < R; j++) {
                start_time = System.currentTimeMillis();
                triangNumList[j] = MR_ApproxTCwithNodeColors(edgesSparkRDD, C);
                end_time = System.currentTimeMillis();
                runTimeList[j] = end_time - start_time;
            }

            Arrays.sort(triangNumList);

            /*Median computation: */
            if (triangNumList.length % 2 == 0)
                median = (triangNumList[triangNumList.length / 2] + triangNumList[triangNumList.length / 2 - 1]) / 2;
            else
                median = triangNumList[triangNumList.length / 2];

            /*Computation of final execution time: */
            long timeSum = 0L;
            for (Long e : runTimeList) {
                timeSum += e;
            }
            final_exe_time = timeSum / runTimeList.length;

            System.out.println("Approximation through node coloring:");
            System.out.println("- Number of triangles (median over " + R + " runs) = " + median);
            System.out.println("- Running time (mean over " + R + " runs) = " + final_exe_time);
            System.out.println("---------------------------------------------------------");
        }
        else {

            long triangles_ExactTC = 0L;
            JavaPairRDD<Integer, Integer> edgesSparkRDD = edges.repartition(32).cache();
            for (int j = 0; j < R; j++) {
                start_time = System.currentTimeMillis();
                triangles_ExactTC = MR_ExactTC(edgesSparkRDD, C);
                end_time = System.currentTimeMillis();
                runTimeList[j] = end_time - start_time;
            }

            long timeSum = 0L;
            for (Long e : runTimeList) {
                timeSum += e;
            }
            final_exe_time = timeSum / runTimeList.length;

            System.out.println("Exact number of triangles:");
            System.out.println("- Number of triangles = " + triangles_ExactTC);
            System.out.println("- Running time (mean over " + R + " runs) = " + final_exe_time);
            System.out.println("---------------------------------------------------------");
        }
    }

    public static Long  MR_ApproxTCwithNodeColors(JavaPairRDD<Integer, Integer> edges, int C){

        Random rnd = new Random();
        int p = 8191;
        int a = rnd.nextInt((p - 1)) + 1;
        int b = rnd.nextInt(p);

        JavaPairRDD<Integer, Long> edgeSet = edges.flatMapToPair(
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
                .flatMapToPair((element) -> {
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

        long numTriangles = edgeSet.map(Tuple2::_2).reduce((x, y) -> y);

        return numTriangles;
    }

    public static Long MR_ExactTC(JavaPairRDD<Integer, Integer> edges, int C){

        Random rnd = new Random();
        int p = 8191;
        long a = rnd.nextInt((p - 1)) + 1;
        long b = rnd.nextInt(p);

        JavaPairRDD<Integer, Long> edgeSet = edges.flatMapToPair(
                (pair) -> {

                    ArrayList<Tuple2<Tuple3<Integer, Integer, Integer>, Tuple2<Integer, Integer>>> pairs = new ArrayList<>();

                    // Color assignment
                    long hC1 = ((a * (long)pair._1() + b) % p) % C;
                    long hC2 = ((a * (long)pair._2() + b) % p) % C;

                    for (int i = 0; i < C; i++){
                        if (hC1 <= hC2 && hC1 <= i){
                            pairs.add(new Tuple2<>(new Tuple3<>((int)hC1, Math.min((int)hC2, i), Math.max((int)hC2, i)), new Tuple2<>(pair._1(), pair._2())));
                        }
                        else if (hC1 > hC2){
                            if (hC1 > i) {
                                pairs.add(new Tuple2<>(new Tuple3<>(Math.min((int)hC2, i), Math.max((int)hC2, i), (int)hC1), new Tuple2<>(pair._1(), pair._2())));
                            }
                            else{
                                pairs.add(new Tuple2<>(new Tuple3<>((int)hC2, (int)hC1, i), new Tuple2<>(pair._1(), pair._2())));
                            }
                        }
                        else{
                            pairs.add(new Tuple2<>(new Tuple3<>(i, (int)hC1, (int)hC2), new Tuple2<>(pair._1(), pair._2())));
                        }
                    }

                    return pairs.iterator();
                })
                .groupByKey()
                .flatMapToPair((element) -> {
                    ArrayList<Tuple2<Integer, Integer>> couple = new ArrayList<>();
                    for (Tuple2<Integer, Integer> e : element._2()) {
                        couple.add(e);
                    }
                    ArrayList<Tuple2<Integer, Long>> sTriangles = new ArrayList<>();
                    sTriangles.add(new Tuple2<>(0, CountTriangles2(couple, element._1(), a, b, p, C)));
                    return sTriangles.iterator();
                })
                .groupByKey()
                .mapValues((it) -> {
                    long sum = 0;
                    for (long c : it){
                        sum += c;
                    }
                    return sum;
                });
        long numTriangles = edgeSet.map(Tuple2::_2).reduce((x, y) -> y);

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
    public static Long CountTriangles2(ArrayList<Tuple2<Integer, Integer>> edgeSet, Tuple3<Integer, Integer, Integer> key, long a, long b, long p, int C) {
        if (edgeSet.size()<3) return 0L;
        HashMap<Integer, HashMap<Integer,Boolean>> adjacencyLists = new HashMap<>();
        HashMap<Integer, Integer> vertexColors = new HashMap<>();
        for (int i = 0; i < edgeSet.size(); i++) {
            Tuple2<Integer,Integer> edge = edgeSet.get(i);
            int u = edge._1();
            int v = edge._2();
            if (vertexColors.get(u) == null) {vertexColors.put(u, (int) ((a*u+b)%p)%C);}
            if (vertexColors.get(v) == null) {vertexColors.put(v, (int) ((a*v+b)%p)%C);}
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
                        if (w>v && (uAdj.get(w)!=null)) {
                            ArrayList<Integer> tcol = new ArrayList<>();
                            tcol.add(vertexColors.get(u));
                            tcol.add(vertexColors.get(v));
                            tcol.add(vertexColors.get(w));
                            Collections.sort(tcol);
                            boolean condition = (tcol.get(0).equals(key._1())) && (tcol.get(1).equals(key._2())) && (tcol.get(2).equals(key._3()));
                            if (condition) {numTriangles++;}
                        }
                    }
                }
            }
        }
        return numTriangles;
    }

}
