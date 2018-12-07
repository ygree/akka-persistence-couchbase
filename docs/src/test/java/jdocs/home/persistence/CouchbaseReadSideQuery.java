package jdocs.home.persistence;

//#imports
import akka.NotUsed;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
import com.couchbase.client.java.document.json.JsonObject;
import com.lightbend.lagom.javadsl.api.ServiceCall;
import com.lightbend.lagom.javadsl.persistence.ReadSide;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.inject.Inject;


//#imports

public interface CouchbaseReadSideQuery {

  interface GreetingService {
    ServiceCall<NotUsed, List<UserGreeting>> userGreetings();
  }

  //#service-impl
  public class GreetingServiceImpl implements GreetingService {

    private final CouchbaseSession session;

    @Inject
    public GreetingServiceImpl(CouchbaseSession couchbaseSession) {
      this.session = couchbaseSession;
    }

    @Override
    public ServiceCall<NotUsed, List<UserGreeting>> userGreetings() {
      return request -> session.get("users-actual-greetings")
          .thenApply(docOpt -> {
            if (docOpt.isPresent()) {
              JsonObject content = docOpt.get().content();
              return content.getNames().stream().map(
                  name -> new UserGreeting(name, content.getString(name))
              ).collect(Collectors.toList());
            } else {
              return Collections.emptyList();
            }
          });
    }
  }
  //#service-impl

  static class Wrap {
    public class GreetingServiceImpl implements GreetingService {

      private final CouchbaseSession session;

      //#register-event-processor
      @Inject
      public GreetingServiceImpl(CouchbaseSession couchbaseSession, ReadSide readSide) {
        this.session = couchbaseSession;
        readSide.register(CouchbaseHelloEventProcessor.HelloEventProcessor.class);
      }
      //#register-event-processor

      @Override
      public ServiceCall<NotUsed, List<UserGreeting>> userGreetings() {
        return request -> session.get("users-actual-greetings")
            .thenApply(docOpt -> {
              if (docOpt.isPresent()) {
                JsonObject content = docOpt.get().content();
                return content.getNames().stream().map(
                    name -> new UserGreeting(name, content.getString(name))
                ).collect(Collectors.toList());
              } else {
                return Collections.emptyList();
              }
            });
      }
    }
  }

}
