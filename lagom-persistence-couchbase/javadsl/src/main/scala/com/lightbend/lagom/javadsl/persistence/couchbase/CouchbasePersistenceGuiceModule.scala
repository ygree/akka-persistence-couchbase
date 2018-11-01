/*
 * Copyright (C) 2018 Lightbend Inc. <http://www.lightbend.com>
 */

package com.lightbend.lagom.javadsl.persistence.couchbase

import com.google.inject.matcher.AbstractMatcher
import com.google.inject.spi.{ InjectionListener, TypeEncounter, TypeListener }
import com.google.inject.{ AbstractModule, TypeLiteral }

/**
 * Guice module for the Couchbase Persistence API.
 *
 * This serves one purpose, to invoke the @PostConstruct annotated init method on
 * InitServiceLocatorHolder, since Guice doesn't support @PostConstruct.
 */
class CouchbasePersistenceGuiceModule extends AbstractModule {

  override def configure(): Unit = {
    initServiceLocatorHolder()
  }

  private def initServiceLocatorHolder(): Unit = {
    val listener: TypeListener = new TypeListener {
      override def hear[I](typeLiteral: TypeLiteral[I], typeEncounter: TypeEncounter[I]): Unit = {
        typeEncounter.register(new InjectionListener[I] {
          override def afterInjection(i: I): Unit = {
            i.asInstanceOf[CouchbasePersistenceModule.InitServiceLocatorHolder].init()
          }
        })
      }
    }
    val matcher = new AbstractMatcher[TypeLiteral[_]] {
      override def matches(typeLiteral: TypeLiteral[_]): Boolean = {
        classOf[CouchbasePersistenceModule.InitServiceLocatorHolder] == typeLiteral.getRawType
      }
    }
    binder.bindListener(matcher, listener)
  }
}
