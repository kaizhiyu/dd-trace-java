package datadog.trace.instrumentation.netty40.client;

import io.netty.channel.CombinedChannelDuplexHandler;

public class HttpClientTracingHandler
    extends CombinedChannelDuplexHandler<
        HttpClientResponseTracingHandler, HttpClientRequestTracingHandler> {

  public HttpClientTracingHandler() {
    super(new HttpClientResponseTracingHandler(), new HttpClientRequestTracingHandler());
  }
}
