/**
 * Copyright (C) 2009-2018 Lightbend Inc. <http://www.lightbend.com>
 */
package akka.stream.alpakka.couchbase.impl

import akka.annotation.InternalApi
import com.couchbase.client.java.document.JsonDocument
import com.couchbase.client.java.document.json.JsonObject
import com.couchbase.client.java.query.{ AsyncN1qlQueryResult, AsyncN1qlQueryRow }
import rx.Observable
import rx.functions.Func1

/**
 * INTERNAL API
 */
@InternalApi
private[couchbase] object RxUtilities {
  val unfoldRows = new Func1[AsyncN1qlQueryResult, Observable[AsyncN1qlQueryRow]] {
    def call(t: AsyncN1qlQueryResult): Observable[AsyncN1qlQueryRow] =
      t.rows()
  }

  val unfoldDocument = new Func1[AsyncN1qlQueryRow, JsonObject] {
    def call(row: AsyncN1qlQueryRow): JsonObject =
      row.value()

  }

  val unfoldJsonObjects = new Func1[AsyncN1qlQueryResult, Observable[JsonObject]] {
    def call(t: AsyncN1qlQueryResult): Observable[JsonObject] =
      t.rows().map(unfoldDocument)
  }

}
