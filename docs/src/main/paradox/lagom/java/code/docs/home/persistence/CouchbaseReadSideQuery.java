package docs.home.persistence;

//#imports
import akka.NotUsed;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
import com.couchbase.client.java.document.json.JsonObject;
import com.lightbend.lagom.javadsl.api.ServiceCall;

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

    private final CouchbaseSession couchbaseSession;

    @Inject
    public GreetingServiceImpl(CouchbaseSession couchbaseSession) {
      this.couchbaseSession = couchbaseSession;
    }

    @Override
    public ServiceCall<NotUsed, List<UserGreeting>> userGreetings() {
      return request -> couchbaseSession.get("users-actual-greetings")
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

  public class GreetingServiceImpl2 implements GreetingService {

    //#register-event-processor
    @Inject
    public GreetingServiceImpl(ReadSide readSide) {

      readSide.register(HelloEventProcessor.class);
    }
    //#register-event-processor

    @Override
    public ServiceCall<NotUsed, List<UserGreeting>> userGreetings() {
      return request -> couchbaseSession.get("users-actual-greetings")
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
