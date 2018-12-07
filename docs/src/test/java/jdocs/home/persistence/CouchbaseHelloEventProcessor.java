package jdocs.home.persistence;

//#imports
import akka.Done;
import akka.stream.alpakka.couchbase.javadsl.CouchbaseSession;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;
import com.lightbend.lagom.javadsl.persistence.AggregateEventTag;
import com.lightbend.lagom.javadsl.persistence.ReadSideProcessor;
import com.lightbend.lagom.javadsl.persistence.couchbase.CouchbaseReadSide;
import org.pcollections.PSequence;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static akka.Done.done;


//#imports

public class CouchbaseHelloEventProcessor {

  class Intial {
    //#initial
    public class HelloEventProcessor extends ReadSideProcessor<HelloEvent> {

      private final CouchbaseReadSide readSide;

      @Inject
      public HelloEventProcessor(CouchbaseReadSide readSide) {
        this.readSide = readSide;
      }

      @Override
      public ReadSideHandler<HelloEvent> buildHandler() {
        // TODO build read side handler
        return null;
      }

      @Override
      public PSequence<AggregateEventTag<HelloEvent>> aggregateTags() {
        // TODO return the tag for the events
        return null;
      }
    }
    //#initial
  }

  public class HelloEventProcessor extends ReadSideProcessor<HelloEvent> {

    private final CouchbaseReadSide readSide;

    @Inject
    public HelloEventProcessor(CouchbaseReadSide readSide) {
      this.readSide = readSide;
    }

    //#tag
    @Override
    public PSequence<AggregateEventTag<HelloEvent>> aggregateTags() {
      return HelloEvent.TAG.allTags();
    }
    //#tag

    //#create-document
    final String DOC_ID = "users-actual-greetings";

    private CompletionStage<Done> createDocument(CouchbaseSession session) {
      return
          session.get(DOC_ID).thenComposeAsync(doc -> {
            if (doc.isPresent()) {
              return CompletableFuture.completedFuture(Done.getInstance());
            }
            return session.insert(JsonDocument.create(DOC_ID, JsonObject.empty()))
                .thenApply(ignore -> Done.getInstance());
          });
    }
    //#create-document

    //#prepare-statements
    private CompletionStage<Done> prepare(CouchbaseSession session, AggregateEventTag<HelloEvent> tag) {
      //TODO do something when read-side is run for each shard
      return CompletableFuture.completedFuture(Done.getInstance());
    }
    //#prepare-statements

    @Override
    public ReadSideHandler<HelloEvent> buildHandler() {
      //#create-builder
      CouchbaseReadSide.ReadSideHandlerBuilder<HelloEvent> builder =
          readSide.builder("all-greetings");
      //#create-builder

      //#register-global-prepare
      builder.setGlobalPrepare(this::createDocument);
      //#register-global-prepare

      //#set-event-handler
      builder.setEventHandler(HelloEvent.GreetingMessageChanged.class, this::processGreetingMessageChanged);
      //#set-event-handler

      //#register-prepare
      builder.setPrepare(this::prepare);
      //#register-prepare

      //#build
      return builder.build();
      //#build
    }

    //#greeting-message-changed
    private CompletionStage<Done> processGreetingMessageChanged(CouchbaseSession session, HelloEvent.GreetingMessageChanged evt) {
      // FIXME support granular upsert in session API to get an atomic upsert here #136
      return session.get(DOC_ID).thenCompose((maybeJson) -> {
        final JsonObject json;
        if (maybeJson.isPresent()) {
          json = maybeJson.get().content();
        } else {
          json = JsonObject.create();
        }
        return session.upsert(JsonDocument.create(DOC_ID, json.put(evt.name, evt.message)));
      }).thenApply(doc -> done());
    }
    //#greeting-message-changed

  }

}
