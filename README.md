Dropbox Load Management Libraries
=================================

This repository contains Go utilities for managing isolation and improving
reliability of multi-tenant systems.  These utilities might be helpful
additions to your production toolbox if:

 1. You program in Go.
 2. Your system is shared by multiple tenants and there is an expectation that
    one tenant's activity on the system should not adversely affect another
    tenant's workload.
 3. There is some way to interpose on the shared resource, like a proxy in front
    of the database or within the database code directly.
 4. Preventing a disastrous worst case behavior is a higher priority than
    extracting the highest performance possible from the system.
 5. You may not have the luxury of designing limitations into your system from
    scratch, but may instead have to progressively apply stricter limits in
    production.

Contained within this repository are utilities pulled from a production system
at Dropbox where they were developed to improve the reliability of a
petabyte-scale storage system used by most of the company.  While the
utilities compose together to provide load management, they are also useful in
isolation.  The specific utilities are:

 - admission_control: Semaphores with explicit timeout and ordering
   properties.
 - load_manager: Combine scorecard, admission control into a coherent
   solution for selectively shedding load.
 - scorecard: Tag traffic and decide what to do with traffic using the tags.

Collectively, these utilities give us the pieces we need to be able to limit
the number of active requests for the tagged traffic.  The benefit of this is
that it can protect processes from overusing shared resources at the risk of
possibly underutilizing idle resources.

Quick Start
===========

We recommend reading through the documentation and interface definitions, but if
you are itching to get started quickly, here's what you have to do:

First, we will need to setup an instance of the load manager in the global state
of our application.  This will allow we to maintain a single load manager
throughout the lifetime of our process.  It looks something like this:

```
var loadManager *load_manager.LoadManager
var loadManagerOnce sync.Once
var mainQueueName = load_manager.QueueName("main")

func LoadManager() *load_manager.LoadManager {
    loadManagerOnce.Do(func() {
        loadManager = load_manager.NewLoadManager(
            map[load_manager.QueueName]ac.AdmissionController{
                mainQueueName: ac.NewAdmissionController(1),
            },
            ac.NewAdmissionController(1),
            scorecard.NewScorecard(scorecard.NoRules()),
            nil, // No canary scorecard
            scorecard.NoTags(), // No default tags.
        )
    })
    return loadManager
}
```

This will give us a single load manager that we can access from anywhere
within our server process; we can then use the load manager to decide whether
to accept and process traffic.  In a typical usage scenario it looks something
like this:

```
// Foo reports the time spent in the request and uses load manager.
func Foo(ctx context.Context, conn net.Conn, data []byte) error {
	maybeHostPort := conn.RemoteAddr().String()
	host, _, _ := net.SplitHostPort(maybeHostPort)
    requestSize := int(math.Log10(len(data)))
    tags := []scorecard.Tag{
        scorecard.MakeStringTag("handler", FooHandlerName),
        scorecard.MakeStringTag("source_ip", host),
        scorecard.MakeIntTag("request_size", requestSize),
    }
    resource := LoadManager().GetResource(ctx, mainQueueName, tags)
    if !resource.Acquired() {
        return fmt.Errorf("Error acquiring resource")
    }
    defer resource.Release()

    // begin original func Foo here
}
```

What we see in this example is that a preamble consisting of just a few lines
can provide some protection for our original, unprotected, function.  The
first few lines build up a set of "tags" for the traffic that describe the
context of the current execution.  These tags could be anything from the name
of the function Foo to an identifier of the end user who initiated the request
way off in some distant microservice.  Tags are quite literally strings that
describe the attributes we'd like to use when deciding to accept or reject a
request.

We feed these tags into the singleton load manager that we instantiated above
to receive a ticket to enter into the function Foo.  Internally, the load
manager will track the number of tickets it issues using admission control and
prevent too many goroutines from concurrently entering.  It will also use the
provided to tags to prevent too many requests with the same tags from entering
simultaneously.  With a ticket in hand, we can then safely execute the
original function Foo, but don't forget to release the ticket for the next
request; a defer is safest for this.

Out of the box, the load manager will only enforce limitations on the number of
concurrent active requests (via the passed AdmissionControllers) with some
additional latency back-pressure based on the configuration of the passed
suspicious AdmissionController.  To change this, we must also instantiate a load
manager with a set of "rules" that describe how we want to shape our traffic
(vis-a-vis a Scorecard). A rule is a pair that consists of a pattern for
matching tags and an integer specifying the number of requests that match the
tag to be concurrently permitted to acquire a ticket.  For example, we could
specify the rule `source_ip:127.0.0.1, 10` to specify that any given client host
is permitted to hold ten tickets concurrently.  An attempt to acquire an
eleventh ticket will be rejected.

For maximum flexibility, the rules permit the use of wild-cards and
conjunctions.  For example, manually providing a "source:X" rule is relatively
straightforward, but as the number of values of X increases, it becomes
toil-some to keep the rules in sync.  Instead, we can provide a rule that
matches any source, like `source_ip:*,10`.  This will allow each tag that
matches the rule, e.g. `source_ip:1.1.1.1` or `source_ip:2.2.2.2`, to have up to
10 concurrent requests.  Thus, the total number of requests permitted will be
the lesser of 10 times the number of distinct source_ips in our system or the
load manager's limit on the number of concurrently-outstanding tickets.

Conjunctions further increase the flexibility of rules by allowing a rule to
match only when a request matches a set of tags.  Such a rule combines the tags
using the ";" operator and could look like this: `source_ip:1.1.1.1;handler:Foo, 1`.
This particular rule would allow the upstream client host to have at most one
concurrent request to the Foo endpoint at any given time.  Of course,
conjunctions may be combined with wild cards to say something like,
`source_ip:*;handler:*,1` which prevent any handler from being invoked more than
once concurrently from any given client host.

Usage Details
======================

Please see [Godoc](https://godoc.org/github.com/dropbox/load_management) for
usage and implementation notes.

Contributions
=============

Contributions to this library are welcome, but because we keep our open source
release in sync with the upstream, not all pull requests will be merged.  If
you are working on this library, please do talk to us in advance to talk
through your intended changes and how we can merge them into the upstream.

To submit a pull request, please sign our [Contributor License
Agreement](https://opensource.dropbox.com/cla/) and indicate in your pull
request that you've signed it.

Further Reading
===============

The ideas contained within these packages are not new. We were inspired to
implement ideas first presented by Facebook in the process of evaluating the
multi-tenancy of Edgestore, our distributed metadata storage (read more
[here](https://blogs.dropbox.com/tech/2016/08/reintroducing-edgestore/)).

The basis for our implementations can be found in the following resources:
 - [Fail at Scale](https://queue.acm.org/detail.cfm?id=2839461)
 - [Balancing Multi-Tenancy and Isolation at 4 Billion QPS](https://youtu.be/dATHiDHS3Mo?t=18m52s)
