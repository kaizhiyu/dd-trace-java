package datadog.trace.instrumentation.java.concurrent;

import static datadog.trace.agent.tooling.bytebuddy.matcher.DDElementMatchers.extendsClass;
import static java.util.Collections.singletonMap;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.nameEndsWith;
import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.namedOneOf;

import com.google.auto.service.AutoService;
import datadog.trace.agent.tooling.Instrumenter;
import datadog.trace.bootstrap.InstrumentationContext;
import datadog.trace.bootstrap.instrumentation.java.concurrent.State;
import datadog.trace.context.TraceScope;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;

/**
 * Generic instrumentation targeting the lifecycle (construct, run, cancel) of RunnableFutures. This
 * is to avoid the complexity of writing instrumentation targeting specific RunnableFuture types for
 * libraries which are often shaded. The type matcher is very specific to avoid unexpected behaviour
 * with unencountered libraries.
 */
@AutoService(Instrumenter.class)
public class RunnableFutureInstrumentation extends Instrumenter.Default {

  public RunnableFutureInstrumentation() {
    super("java_concurrent", "runnable-future");
  }

  @Override
  public ElementMatcher<? super TypeDescription> typeMatcher() {
    return extendsClass(
        namedOneOf("java.util.concurrent.FutureTask")
            .or(nameEndsWith("netty.util.concurrent.PromiseTask"))
            .or(nameEndsWith("com.google.common.util.concurrent.TrustedListenableFutureTask")));
  }

  @Override
  public Map<String, String> contextStore() {
    return singletonMap("java.util.concurrent.RunnableFuture", State.class.getName());
  }

  @Override
  public Map<? extends ElementMatcher<? super MethodDescription>, String> transformers() {
    Map<ElementMatcher<? super MethodDescription>, String> transformers = new HashMap<>(4);
    transformers.put(isMethod().and(named("run")), getClass().getName() + "$Run");
    transformers.put(isMethod().and(named("cancel")), getClass().getName() + "$Cancel");
    return Collections.unmodifiableMap(transformers);
  }

  public static final class Run {
    @Advice.OnMethodEnter
    public static <T> TraceScope before(@Advice.This RunnableFuture<T> runnableFuture) {
      State state =
          InstrumentationContext.get(RunnableFuture.class, State.class).get(runnableFuture);
      if (null != state) {
        TraceScope.Continuation continuation = state.getAndResetContinuation();
        if (null != continuation) {
          return continuation.activate();
        }
      }
      return null;
    }

    @Advice.OnMethodExit(onThrowable = Throwable.class)
    public static <T> void after(
        @Advice.Enter TraceScope scope, @Advice.This RunnableFuture<T> runnableFuture) {
      if (null != scope) {
        scope.close();
      }
    }
  }

  public static final class Cancel {
    @Advice.OnMethodExit
    public static <T> void cancel(@Advice.This RunnableFuture<T> runnableFuture) {
      State state =
          InstrumentationContext.get(RunnableFuture.class, State.class).get(runnableFuture);
      if (null != state) {
        state.closeContinuation();
      }
    }
  }
}
