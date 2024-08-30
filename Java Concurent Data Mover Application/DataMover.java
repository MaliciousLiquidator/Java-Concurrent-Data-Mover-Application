import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DataMover {
    private static int[] data;
    private static List<Thread> movers = new ArrayList<>();
    private static int moveTime;

    public static void main(String[] args) {
        int numThreads = args.length > 1 ? args.length - 1 : 4;
        moveTime = args.length > 0 ? Integer.parseInt(args[0]) : 123;
        data = new int[numThreads];
        for (int i = 0; i < numThreads; i++) {
            data[i] = i * 1000;
        }

        for (int i = 0; i < numThreads; i++) {
            int threadIndex = i;
            int waitTime = args.length > i + 1 ? Integer.parseInt(args[i + 1]) : new int[]{111, 256, 404}[i];
            Thread thread = new Thread(() -> {
                try {
                    for (int j = 0; j < 10; j++) {
                        Thread.sleep(waitTime);
                        synchronized (DataMover.class) {
                            data[threadIndex] -= threadIndex;
                            System.out.println("#" + threadIndex + ": data " + threadIndex + " == " + data[threadIndex]);
                            Thread.sleep(moveTime);
                            int nextIndex = (threadIndex + 1) % numThreads;
                            data[nextIndex] += threadIndex;
                            System.out.println("#" + threadIndex + ": data " + nextIndex + " -> " + data[nextIndex]);
                        }
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
            movers.add(thread);
            thread.start();
        }

        for (Thread mover : movers) {
            try {
                mover.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        System.out.println("Final data array: " + Arrays.toString(data));
    }
}
