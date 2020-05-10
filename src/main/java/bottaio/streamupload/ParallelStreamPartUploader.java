package bottaio.streamupload;

import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static bottaio.streamupload.StreamTransferManager.Config;

@Slf4j
public class ParallelStreamPartUploader implements StreamPartUploader {
  private final StreamPartUploader uploader;
  private final BlockingQueue<StreamPart> queue;
  private final ExecutorService threadPool;
  private final AtomicInteger workersCount;

  private volatile boolean aborted;
  private volatile boolean finished;

  public ParallelStreamPartUploader(Config config, StreamPartUploader uploader) {
    this.uploader = uploader;
    this.queue = new LinkedBlockingQueue<>(config.getQueueCapacity());

    this.threadPool = Executors.newFixedThreadPool(config.getNumWorkers());
    this.workersCount = new AtomicInteger(config.getNumWorkers());
    for (int i = 0; i < config.getNumWorkers(); i++) {
      spawnWorker();
    }
  }

  private void spawnWorker() {
    threadPool.execute(new Worker());
  }

  @Override
  public void initialize() {
    uploader.initialize();
  }

  @Override
  public void upload(StreamPart part) {
    boolean success = false;

    while (!success && !aborted) {
      try {
        success = queue.offer(part, 5, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void complete() {
    finished = true;
    while (workersCount.get() > 0) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        log.info("Interrupted");
      }
    }
    threadPool.shutdown();
    uploader.complete();
  }

  @Override
  public void abort() {
    aborted = true;
  }

  class Worker implements Runnable {
    @Override
    public void run() {
      try {
        while (!aborted && (!finished || !queue.isEmpty())) {
          StreamPart part = null;
          try {
            part = queue.poll(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            log.info("Nothing to run");
          }

          if (part != null) {
            uploader.upload(part);
          }
        }
      } finally {
        workersCount.decrementAndGet();
      }
    }
  }
}
