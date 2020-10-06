package datadog.trace.instrumentation.java.concurrent;

import static datadog.trace.agent.tooling.bytebuddy.matcher.DDElementMatchers.hasInterface;
import static net.bytebuddy.matcher.ElementMatchers.isMethod;
import static net.bytebuddy.matcher.ElementMatchers.named;

import com.google.auto.service.AutoService;
import datadog.trace.agent.tooling.Instrumenter;
import datadog.trace.bootstrap.InstrumentationContext;
import datadog.trace.bootstrap.instrumentation.java.concurrent.ExecutionContext;
import datadog.trace.bootstrap.instrumentation.java.concurrent.State;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RunnableFuture;
import net.bytebuddy.asm.Advice;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.type.TypeDescription;
import net.bytebuddy.matcher.ElementMatcher;
import net.bytebuddy.matcher.ElementMatchers;

@AutoService(Instrumenter.class)
public class RejectedExecutionHandlerInstrumentation extends Instrumenter.Default {
  public RejectedExecutionHandlerInstrumentation() {
    super("java_concurrent", "rejected-execution-handler");
  }

  @Override
  public ElementMatcher<? super TypeDescription> typeMatcher() {
    return hasInterface(named("java.util.concurrent.RejectedExecutionHandler"));
  }

  @Override
  public Map<String, String> contextStore() {
    Map<String, String> contextStore = new HashMap<>(4);
    contextStore.put("java.util.concurrent.RunnableFuture", State.class.getName());
    contextStore.put("java.util.concurrent.ForkJoinTask", State.class.getName());
    return Collections.unmodifiableMap(contextStore);
  }

  @Override
  public Map<? extends ElementMatcher<? super MethodDescription>, String> transformers() {
    return Collections.singletonMap(
        isMethod()
            .and(named("rejectedExecution"))
            .and(ElementMatchers.takesArgument(0, named("java.util.concurrent.Runnable"))),
        getClass().getName() + "$Reject");
  }

  public static final class Reject {
    @Advice.OnMethodEnter
    public static void reject(@Advice.Argument(value = 0, readOnly = false) Runnable runnable) {
      if (runnable instanceof ExecutionContext) {
        ExecutionContext executionContext = (ExecutionContext) runnable;
        executionContext.cancel();
        runnable = executionContext.unwrap();
      } else if (runnable instanceof RunnableFuture) {
        State state =
            InstrumentationContext.get(RunnableFuture.class, State.class)
                .get((RunnableFuture<?>) runnable);
        if (null != state) {
          state.closeContinuation();
        }
      } else if (runnable instanceof ForkJoinTask) {
        State state =
            InstrumentationContext.get(ForkJoinTask.class, State.class)
                .get((ForkJoinTask<?>) runnable);
        if (null != state) {
          state.closeContinuation();
        }
      } else if (null != runnable) {
        ExecutionContext.clear(runnable);
      }
    }
  }
}
