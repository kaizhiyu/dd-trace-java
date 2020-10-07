import com.google.common.util.concurrent.MoreExecutors
import datadog.trace.agent.test.AgentTestRunner
import datadog.trace.agent.test.utils.ConfigUtils
import datadog.trace.api.Trace
import datadog.trace.core.DDSpan
import io.netty.channel.DefaultEventLoopGroup
import io.netty.channel.epoll.EpollEventLoopGroup
import io.netty.channel.local.LocalEventLoopGroup
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.util.concurrent.DefaultEventExecutorGroup
import io.netty.util.concurrent.UnorderedThreadPoolEventExecutor
import org.apache.tomcat.util.threads.TaskQueue
import org.eclipse.jetty.util.component.AbstractLifeCycle
import org.eclipse.jetty.util.thread.MonitoredQueuedThreadPool
import org.eclipse.jetty.util.thread.QueuedThreadPool
import org.eclipse.jetty.util.thread.ReservedThreadExecutor
import spock.lang.Shared

import java.lang.reflect.InvocationTargetException
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.ArrayBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutionException
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.ForkJoinPool
import java.util.concurrent.ForkJoinTask
import java.util.concurrent.Future
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

import static datadog.trace.bootstrap.instrumentation.api.AgentTracer.activeScope
import static org.junit.Assume.assumeTrue

class ExecutorInstrumentationTest extends AgentTestRunner {

  @Shared
  boolean isLinux = System.getProperty("os.name").toLowerCase().contains("linux")

  static {
    ConfigUtils.updateConfig {
      System.setProperty("dd.trace.executors", "ExecutorInstrumentationTest\$CustomThreadPoolExecutor")
    }
  }

  @Shared
  def executeRunnable = { e, c -> e.execute((Runnable) c) }
  @Shared
  def executeForkJoinTask = { e, c -> e.execute((ForkJoinTask) c) }
  @Shared
  def submitRunnable = { e, c -> e.submit((Runnable) c) }
  @Shared
  def submitCallable = { e, c -> e.submit((Callable) c) }
  @Shared
  def submitForkJoinTask = { e, c -> e.submit((ForkJoinTask) c) }
  @Shared
  def invokeAll = { e, c -> e.invokeAll([(Callable) c]) }
  @Shared
  def invokeAllTimeout = { e, c -> e.invokeAll([(Callable) c], 10, TimeUnit.SECONDS) }
  @Shared
  def invokeAny = { e, c -> e.invokeAny([(Callable) c]) }
  @Shared
  def invokeAnyTimeout = { e, c -> e.invokeAny([(Callable) c], 10, TimeUnit.SECONDS) }
  @Shared
  def invokeForkJoinTask = { e, c -> e.invoke((ForkJoinTask) c) }
  @Shared
  def scheduleRunnable = { e, c -> e.schedule((Runnable) c, 10, TimeUnit.MILLISECONDS) }
  @Shared
  def scheduleCallable = { e, c -> e.schedule((Callable) c, 10, TimeUnit.MILLISECONDS) }

  def "#poolImpl '#name' propagates"() {
    setup:
    assumeTrue(poolImpl != null) // skip for Java 7 CompletableFuture, netty EPoll
    def pool = poolImpl
    def m = method
    if (pool instanceof AbstractLifeCycle) {
      ((AbstractLifeCycle)pool).start()
    }

    new Runnable() {
      @Override
      @Trace(operationName = "parent")
      void run() {
        activeScope().setAsyncPropagation(true)
        // this child will have a span
        m(pool, new JavaAsyncChild())
        // this child won't
        m(pool, new JavaAsyncChild(false, false))
        blockUntilChildSpansFinished(1)
      }
    }.run()

    TEST_WRITER.waitForTraces(1)
    List<DDSpan> trace = TEST_WRITER.get(0)

    expect:
    TEST_WRITER.size() == 1
    trace.size() == 2
    trace.get(0).operationName == "parent"
    trace.get(1).operationName == "asyncChild"
    trace.get(1).parentId == trace.get(0).spanId

    cleanup:
    if (pool?.hasProperty("shutdown")) {
      pool?.shutdown()
    } else if (pool instanceof AbstractLifeCycle) {
      ((AbstractLifeCycle)pool).stop()
    }

    // Unfortunately, there's no simple way to test the cross product of methods/pools.
    where:
    name                     | method              | poolImpl
    "execute Runnable"       | executeRunnable     | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "submit Runnable"        | submitRunnable      | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "submit Callable"        | submitCallable      | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAll"              | invokeAll           | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAll with timeout" | invokeAllTimeout    | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAny"              | invokeAny           | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAny with timeout" | invokeAnyTimeout    | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))

    // Scheduled executor has additional methods and also may get disabled because it wraps tasks
    "execute Runnable"       | executeRunnable     | new ScheduledThreadPoolExecutor(1)
    "submit Runnable"        | submitRunnable      | new ScheduledThreadPoolExecutor(1)
    "submit Callable"        | submitCallable      | new ScheduledThreadPoolExecutor(1)
    "invokeAll"              | invokeAll           | new ScheduledThreadPoolExecutor(1)
    "invokeAll with timeout" | invokeAllTimeout    | new ScheduledThreadPoolExecutor(1)
    "invokeAny"              | invokeAny           | new ScheduledThreadPoolExecutor(1)
    "invokeAny with timeout" | invokeAnyTimeout    | new ScheduledThreadPoolExecutor(1)
    "schedule Runnable"      | scheduleRunnable    | new ScheduledThreadPoolExecutor(1)
    "schedule Callable"      | scheduleCallable    | new ScheduledThreadPoolExecutor(1)

    // ForkJoinPool has additional set of method overloads for ForkJoinTask to deal with
    "execute Runnable"       | executeRunnable     | new ForkJoinPool()
    "execute ForkJoinTask"   | executeForkJoinTask | new ForkJoinPool()
    "submit Runnable"        | submitRunnable      | new ForkJoinPool()
    "submit Callable"        | submitCallable      | new ForkJoinPool()
    "submit ForkJoinTask"    | submitForkJoinTask  | new ForkJoinPool()
    "invoke ForkJoinTask"    | invokeForkJoinTask  | new ForkJoinPool()
    "invokeAll"              | invokeAll           | new ForkJoinPool()
    "invokeAll with timeout" | invokeAllTimeout    | new ForkJoinPool()
    "invokeAny"              | invokeAny           | new ForkJoinPool()
    "invokeAny with timeout" | invokeAnyTimeout    | new ForkJoinPool()

    // CustomThreadPoolExecutor would normally be disabled except enabled above.
    "execute Runnable"       | executeRunnable     | new CustomThreadPoolExecutor()
    "submit Runnable"        | submitRunnable      | new CustomThreadPoolExecutor()
    "submit Callable"        | submitCallable      | new CustomThreadPoolExecutor()
    "invokeAll"              | invokeAll           | new CustomThreadPoolExecutor()
    "invokeAll with timeout" | invokeAllTimeout    | new CustomThreadPoolExecutor()
    "invokeAny"              | invokeAny           | new CustomThreadPoolExecutor()
    "invokeAny with timeout" | invokeAnyTimeout    | new CustomThreadPoolExecutor()

    // Internal executor used by CompletableFuture
    "execute Runnable"       | executeRunnable     | java7SafeCompletableFutureThreadPerTaskExecutor()

    // java.util.concurrent.Executors$FinalizableDelegatedExecutorService
    "execute Runnable"       | executeRunnable     | Executors.newSingleThreadExecutor()
    "submit Runnable"        | submitRunnable      | Executors.newSingleThreadExecutor()
    "submit Callable"        | submitCallable      | Executors.newSingleThreadExecutor()
    "invokeAll"              | invokeAll           | Executors.newSingleThreadExecutor()
    "invokeAll with timeout" | invokeAllTimeout    | Executors.newSingleThreadExecutor()
    "invokeAny"              | invokeAny           | Executors.newSingleThreadExecutor()
    "invokeAny with timeout" | invokeAnyTimeout    | Executors.newSingleThreadExecutor()

    // java.util.concurrent.Executors$DelegatedExecutorService
    "execute Runnable"       | executeRunnable     | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "submit Runnable"        | submitRunnable      | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "submit Callable"        | submitCallable      | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "invokeAll"              | invokeAll           | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "invokeAll with timeout" | invokeAllTimeout    | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "invokeAny"              | invokeAny           | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "invokeAny with timeout" | invokeAnyTimeout    | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())


    "execute Runnable"       | executeRunnable     | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "submit Runnable"        | submitRunnable      | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "submit Callable"        | submitCallable      | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "invokeAll"              | invokeAll           | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "invokeAll with timeout" | invokeAllTimeout    | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "invokeAny"              | invokeAny           | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "invokeAny with timeout" | invokeAnyTimeout    | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "schedule Runnable"      | scheduleRunnable    | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "schedule Callable"      | scheduleCallable    | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())

    // jetty
    "execute Runnable"       | executeRunnable     | new MonitoredQueuedThreadPool(8)
    "execute Runnable"       | executeRunnable     | new QueuedThreadPool(8)
    "execute Runnable"       | executeRunnable     | new ReservedThreadExecutor(Executors.newSingleThreadExecutor(), 1)

    // tomcat
    "execute Runnable"       | executeRunnable     | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "submit Runnable"        | submitRunnable      | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "submit Callable"        | submitCallable      | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "invokeAll"              | invokeAll           | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "invokeAll with timeout" | invokeAllTimeout    | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "invokeAny"              | invokeAny           | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "invokeAny with timeout" | invokeAnyTimeout    | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())

    // guava
    "execute Runnable"       | executeRunnable     | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "submit Runnable"        | submitRunnable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "submit Callable"        | submitCallable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "invokeAll"              | invokeAll           | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "invokeAll with timeout" | invokeAllTimeout    | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "invokeAny"              | invokeAny           | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "invokeAny with timeout" | invokeAnyTimeout    | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())

    "execute Runnable"       | executeRunnable     | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "submit Runnable"        | submitRunnable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "submit Callable"        | submitCallable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "invokeAll"              | invokeAll           | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "invokeAll with timeout" | invokeAllTimeout    | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "invokeAny"              | invokeAny           | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "invokeAny with timeout" | invokeAnyTimeout    | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "schedule Runnable"      | scheduleRunnable    | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "schedule Callable"      | scheduleCallable    | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())

    // netty

    "execute Runnable"       | executeRunnable     | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "submit Runnable"        | submitRunnable      | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "submit Callable"        | submitCallable      | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAll"              | invokeAll           | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAll with timeout" | invokeAllTimeout    | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAny"              | invokeAny           | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "invokeAny with timeout" | invokeAnyTimeout    | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))

    "execute Runnable"       | executeRunnable     | new UnorderedThreadPoolEventExecutor(1).next()
    "submit Runnable"        | submitRunnable      | new UnorderedThreadPoolEventExecutor(1).next()
    "submit Callable"        | submitCallable      | new UnorderedThreadPoolEventExecutor(1).next()
    "invokeAll"              | invokeAll           | new UnorderedThreadPoolEventExecutor(1).next()
    "invokeAll with timeout" | invokeAllTimeout    | new UnorderedThreadPoolEventExecutor(1).next()
    "invokeAny"              | invokeAny           | new UnorderedThreadPoolEventExecutor(1).next()
    "invokeAny with timeout" | invokeAnyTimeout    | new UnorderedThreadPoolEventExecutor(1).next()
    "schedule Runnable"      | scheduleRunnable    | new UnorderedThreadPoolEventExecutor(1).next()
    "schedule Callable"      | scheduleCallable    | new UnorderedThreadPoolEventExecutor(1).next()

    // Scheduled executor has additional methods and also may get disabled because it wraps tasks
    "execute Runnable"       | executeRunnable     | new DefaultEventExecutorGroup(1).next()
    "submit Runnable"        | submitRunnable      | new DefaultEventExecutorGroup(1).next()
    "submit Callable"        | submitCallable      | new DefaultEventExecutorGroup(1).next()
    "invokeAll"              | invokeAll           | new DefaultEventExecutorGroup(1).next()
    "invokeAll with timeout" | invokeAllTimeout    | new DefaultEventExecutorGroup(1).next()
    "invokeAny"              | invokeAny           | new DefaultEventExecutorGroup(1).next()
    "invokeAny with timeout" | invokeAnyTimeout    | new DefaultEventExecutorGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new DefaultEventExecutorGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new DefaultEventExecutorGroup(1).next()

    "execute Runnable"       | executeRunnable     | new DefaultEventLoopGroup(1).next()
    "submit Runnable"        | submitRunnable      | new DefaultEventLoopGroup(1).next()
    "submit Callable"        | submitCallable      | new DefaultEventLoopGroup(1).next()
    "invokeAll"              | invokeAll           | new DefaultEventLoopGroup(1).next()
    "invokeAll with timeout" | invokeAllTimeout    | new DefaultEventLoopGroup(1).next()
    "invokeAny"              | invokeAny           | new DefaultEventLoopGroup(1).next()
    "invokeAny with timeout" | invokeAnyTimeout    | new DefaultEventLoopGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new DefaultEventLoopGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new DefaultEventLoopGroup(1).next()

    "execute Runnable"       | executeRunnable     | new NioEventLoopGroup(1).next()
    "submit Runnable"        | submitRunnable      | new NioEventLoopGroup(1).next()
    "submit Callable"        | submitCallable      | new NioEventLoopGroup(1).next()
    "invokeAll"              | invokeAll           | new NioEventLoopGroup(1).next()
    "invokeAll with timeout" | invokeAllTimeout    | new NioEventLoopGroup(1).next()
    "invokeAny"              | invokeAny           | new NioEventLoopGroup(1).next()
    "invokeAny with timeout" | invokeAnyTimeout    | new NioEventLoopGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new NioEventLoopGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new NioEventLoopGroup(1).next()

    "execute Runnable"       | executeRunnable     | epollExecutor()
    "submit Runnable"        | submitRunnable      | epollExecutor()
    "submit Callable"        | submitCallable      | epollExecutor()
    "invokeAll"              | invokeAll           | epollExecutor()
    "invokeAll with timeout" | invokeAllTimeout    | epollExecutor()
    "invokeAny"              | invokeAny           | epollExecutor()
    "invokeAny with timeout" | invokeAnyTimeout    | epollExecutor()
    "schedule Runnable"      | scheduleRunnable    | epollExecutor()
    "schedule Callable"      | scheduleCallable    | epollExecutor()

    // ignore deprecation
    "execute Runnable"       | executeRunnable     | new LocalEventLoopGroup(1).next()
    "submit Runnable"        | submitRunnable      | new LocalEventLoopGroup(1).next()
    "submit Callable"        | submitCallable      | new LocalEventLoopGroup(1).next()
    "invokeAll"              | invokeAll           | new LocalEventLoopGroup(1).next()
    "invokeAll with timeout" | invokeAllTimeout    | new LocalEventLoopGroup(1).next()
    "invokeAny"              | invokeAny           | new LocalEventLoopGroup(1).next()
    "invokeAny with timeout" | invokeAnyTimeout    | new LocalEventLoopGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new LocalEventLoopGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new LocalEventLoopGroup(1).next()

  }

  def "#poolImpl '#name' reports after canceled jobs"() {
    setup:
    assumeTrue(poolImpl != null) // skip for Java 7 CompletableFuture, netty EPoll
    def pool = poolImpl
    def m = method
    List<JavaAsyncChild> children = new ArrayList<>()
    List<Future> jobFutures = new ArrayList<>()

    new Runnable() {
      @Override
      @Trace(operationName = "parent")
      void run() {
        activeScope().setAsyncPropagation(true)
        try {
          for (int i = 0; i < 20; ++i) {
            // Our current instrumentation instrumentation does not behave very well
            // if we try to reuse Callable/Runnable. Namely we would be getting 'orphaned'
            // child traces sometimes since state can contain only one continuation - and
            // we do not really have a good way for attributing work to correct parent span
            // if we reuse Callable/Runnable.
            // Solution for now is to never reuse a Callable/Runnable.
            final JavaAsyncChild child = new JavaAsyncChild(false, true)
            children.add(child)
            try {
              Future f = m(pool, child)
              jobFutures.add(f)
            } catch (InvocationTargetException e) {
              throw e.getCause()
            }
          }
        } catch (RejectedExecutionException e) {
        }

        for (Future f : jobFutures) {
          f.cancel(false)
        }
        for (JavaAsyncChild child : children) {
          child.unblock()
        }
      }
    }.run()

    TEST_WRITER.waitForTraces(1)

    expect:
    // FIXME: we should improve this test to make sure continuations are actually closed
    TEST_WRITER.size() == 1

    where:
    name                | method           | poolImpl
    "submit Runnable"   | submitRunnable   | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "submit Callable"   | submitCallable   | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))

    // Scheduled executor has additional methods and also may get disabled because it wraps tasks
    "submit Runnable"   | submitRunnable   | new ScheduledThreadPoolExecutor(1)
    "submit Callable"   | submitCallable   | new ScheduledThreadPoolExecutor(1)
    "schedule Runnable" | scheduleRunnable | new ScheduledThreadPoolExecutor(1)
    "schedule Callable" | scheduleCallable | new ScheduledThreadPoolExecutor(1)

    // ForkJoinPool has additional set of method overloads for ForkJoinTask to deal with
    "submit Runnable"   | submitRunnable   | new ForkJoinPool()
    "submit Callable"   | submitCallable   | new ForkJoinPool()

    // java.util.concurrent.Executors$FinalizableDelegatedExecutorService
    "submit Runnable"        | submitRunnable      | Executors.newSingleThreadExecutor()
    "submit Callable"        | submitCallable      | Executors.newSingleThreadExecutor()

    // java.util.concurrent.Executors$DelegatedExecutorService
    "submit Runnable"        | submitRunnable      | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())
    "submit Callable"        | submitCallable      | Executors.unconfigurableExecutorService(Executors.newSingleThreadExecutor())


    "submit Runnable"        | submitRunnable      | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "submit Callable"        | submitCallable      | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "schedule Runnable"      | scheduleRunnable    | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())
    "schedule Callable"      | scheduleCallable    | Executors.unconfigurableScheduledExecutorService(Executors.newSingleThreadScheduledExecutor())

    // tomcat
    "submit Runnable"        | submitRunnable      | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())
    "submit Callable"        | submitCallable      | new org.apache.tomcat.util.threads.ThreadPoolExecutor(1, 1, 5, TimeUnit.SECONDS, new TaskQueue())

    // guava
    "submit Runnable"        | submitRunnable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())
    "submit Callable"        | submitCallable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor())

    "submit Runnable"        | submitRunnable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "submit Callable"        | submitCallable      | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "schedule Runnable"      | scheduleRunnable    | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())
    "schedule Callable"      | scheduleCallable    | MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor())

    // netty
    "submit Runnable"        | submitRunnable      | new UnorderedThreadPoolEventExecutor(1).next()
    "submit Callable"        | submitCallable      | new UnorderedThreadPoolEventExecutor(1).next()
    "schedule Runnable"      | scheduleRunnable    | new UnorderedThreadPoolEventExecutor(1).next()
    "schedule Callable"      | scheduleCallable    | new UnorderedThreadPoolEventExecutor(1).next()

    "submit Runnable"        | submitRunnable      | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))
    "submit Callable"        | submitCallable      | new ThreadPoolExecutor(1, 1, 1000, TimeUnit.NANOSECONDS, new ArrayBlockingQueue<Runnable>(1))

    // Scheduled executor has additional methods and also may get disabled because it wraps tasks
    "submit Runnable"        | submitRunnable      | new DefaultEventExecutorGroup(1).next()
    "submit Callable"        | submitCallable      | new DefaultEventExecutorGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new DefaultEventExecutorGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new DefaultEventExecutorGroup(1).next()

    "submit Runnable"        | submitRunnable      | new DefaultEventLoopGroup(1).next()
    "submit Callable"        | submitCallable      | new DefaultEventLoopGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new DefaultEventLoopGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new DefaultEventLoopGroup(1).next()

    "submit Runnable"        | submitRunnable      | new NioEventLoopGroup(1).next()
    "submit Callable"        | submitCallable      | new NioEventLoopGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new NioEventLoopGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new NioEventLoopGroup(1).next()

    "submit Runnable"        | submitRunnable      | epollExecutor()
    "submit Callable"        | submitCallable      | epollExecutor()
    "schedule Runnable"      | scheduleRunnable    | epollExecutor()
    "schedule Callable"      | scheduleCallable    | epollExecutor()

    // ignore deprecation
    "submit Runnable"        | submitRunnable      | new LocalEventLoopGroup(1).next()
    "submit Callable"        | submitCallable      | new LocalEventLoopGroup(1).next()
    "schedule Runnable"      | scheduleRunnable    | new LocalEventLoopGroup(1).next()
    "schedule Callable"      | scheduleCallable    | new LocalEventLoopGroup(1).next()
  }

  private static Executor java7SafeCompletableFutureThreadPerTaskExecutor() {
    try {
      return new CompletableFuture.ThreadPerTaskExecutor()
    } catch (NoClassDefFoundError e) {
      return null
    }
  }

  static class CustomThreadPoolExecutor extends AbstractExecutorService {
    volatile running = true
    def workQueue = new LinkedBlockingQueue<Runnable>(10)

    def worker = new Runnable() {
      void run() {
        try {
          while (running) {
            def runnable = workQueue.take()
            runnable.run()
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt()
        } catch (Exception e) {
          e.printStackTrace()
        }
      }
    }

    def workerThread = new Thread(worker, "ExecutorTestThread")

    private CustomThreadPoolExecutor() {
      workerThread.start()
    }

    @Override
    void shutdown() {
      running = false
      workerThread.interrupt()
    }

    @Override
    List<Runnable> shutdownNow() {
      running = false
      workerThread.interrupt()
      return []
    }

    @Override
    boolean isShutdown() {
      return !running
    }

    @Override
    boolean isTerminated() {
      return workerThread.isAlive()
    }

    @Override
    boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
      workerThread.join(unit.toMillis(timeout))
      return true
    }

    @Override
    def <T> Future<T> submit(Callable<T> task) {
      def future = newTaskFor(task)
      execute(future)
      return future
    }

    @Override
    def <T> Future<T> submit(Runnable task, T result) {
      def future = newTaskFor(task, result)
      execute(future)
      return future
    }

    @Override
    Future<?> submit(Runnable task) {
      def future = newTaskFor(task, null)
      execute(future)
      return future
    }

    @Override
    def <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
      return super.invokeAll(tasks)
    }

    @Override
    def <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
      return super.invokeAll(tasks)
    }

    @Override
    def <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
      return super.invokeAny(tasks)
    }

    @Override
    def <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
      return super.invokeAny(tasks)
    }

    @Override
    void execute(Runnable command) {
      workQueue.put(command)
    }
  }

  def epollExecutor() {
    // EPoll only works on linux
    isLinux ? new EpollEventLoopGroup(1).next() : null
  }
}
