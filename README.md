# Couchbase Plugins for Akka Persistence

Replicated [Akka Persistence](https://doc.akka.io/docs/akka/current/scala/persistence.html) journal and snapshot 
store for [Couchbase](https://www.couchbase.com).

Note that this project is in early access, see section below for details about what is more or less complete and
what is in progress.

For questions please use the [discuss.akka.io](https://discuss.akka.io). Tag any new questions with `akka-persistence` and `couchbase`.

[![Build Status](https://travis-ci.org/akka/akka-persistence-couchbase.svg?branch=master)](https://travis-ci.org/akka/akka-persistence-couchbase)

## Project status

The plugins are available in an early access mode, we will publish milestones as we work towards a 1.0 release.

Current state:

*Write Journal* implemented with minor details in progress, should work but not production ready
*Snapshot Store* POC implementation in place, not hardened, expect changes
*Read Journal*  implemented with minor details in progress, should work but not production ready
*Lagom support* POC implementation in place, not hardened, expect changes


### Caveats

 * In preview status - not production ready
 * Performance and failure scenario testing has not yet been done
 * Smaller schema changes will likely happen and there will not be any migration tools provided as long as
   project releases are milestones


## License

Akka Persistence Couchbase is Open Source and available under the Apache 2 License.