import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.List;
import java.util.ArrayList;
import java.util.Random;

public class DataMover2 {
    private static AtomicInteger arrivalCount = new AtomicInteger();
    private static AtomicInteger totalSent = new AtomicInteger();
    private static AtomicInteger totalArrived = new AtomicInteger();
    private static ExecutorService pool;
    private static List<BlockingQueue<Integer>> queues;
    private static List<Future<DataMover2Result>> moverResults;
    private static List<Integer> discards = new ArrayList<>();
    private static Random random = new Random();

    public static void main(String[] args) {
        int numThreads = args.length > 0 ? args.length : 4;
        int totalValues = 5 * numThreads;
        int[] waitTimes = new int[numThreads];
        for (int i = 0; i < numThreads; i++) {
            waitTimes[i] = args.length > i ? Integer.parseInt(args[i]) : new int[] {123, 111, 256, 404}[i];
        }

        pool = Executors.newFixedThreadPool(Math.min(numThreads, 100));
        queues = new ArrayList<>();
        moverResults = new ArrayList<>();
        for (int i = 0; i < numThreads; i++) {
            queues.add(new LinkedBlockingQueue<>());
        }

        for (int i = 0; i < numThreads; i++) {
            int finalI = i;
            Future<DataMover2Result> future = pool.submit(new Callable<DataMover2Result>() {
                @Override
                public DataMover2Result call() throws Exception {
                    DataMover2Result result = new DataMover2Result();
                    int producedCount = 0;
                    while (producedCount < totalValues) {
                        int x = random.nextInt(10001);
                        queues.get(finalI).put(x);
                        totalSent.addAndGet(x);
                        System.out.printf("total %d/%d | #%d sends %d\n", arrivalCount.get(), totalValues, finalI, x);
                        producedCount++;

                        Integer received;
                        try {
                            received = queues.get((finalI + numThreads - 1) % numThreads).poll(random.nextInt(701) + 300, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            continue;
                        }

                        if (received == null) {
                            System.out.printf("total %d/%d | #%d got nothing...\n", arrivalCount.get(), totalValues, finalI);
                            continue;
                        }

                        if (received % numThreads == finalI) {
                            arrivalCount.incrementAndGet();
                            result.count++;
                            result.data += received;
                            System.out.printf("total %d/%d | #%d got %d\n", arrivalCount.get(), totalValues, finalI, received);
                        } else {
                            queues.get(finalI).put(received - 1);
                            result.forwarded++;
                            System.out.printf("total %d/%d | #%d forwards %d [%d]\n", arrivalCount.get(), totalValues, finalI, received, received % numThreads);
                        }

                        Thread.sleep(waitTimes[finalI]);
                    }
                    return result;
                }
            });
            moverResults.add(future);
        }

        pool.shutdown();
        try {
            pool.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Future<DataMover2Result> future : moverResults) {
            try {
                DataMover2Result result = future.get();
                totalArrived.addAndGet(result.data + result.forwarded);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }

        queues.forEach(q -> q.drainTo(discards));
        int discardedSum = discards.stream().mapToInt(Integer::intValue).sum();

        System.out.println("discarded " + discards + " = " + discardedSum);
        if (totalSent.get() == totalArrived.get() + discardedSum) {
            System.out.println("sent " + totalSent.get() + " === got " + totalArrived.get() + " = " + discardedSum);
        } else {
            System.out.println("WRONG sent " + totalSent.get() + " !== got " + totalArrived.get() + " = " + discardedSum);
        }
    }

    static class DataMover2Result {
        public int count;
        public int data;
        public int forwarded;
    }
}
