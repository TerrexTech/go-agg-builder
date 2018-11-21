Aggregate-Builder
---

This library constructs Aggregates' Domain by reading Event-Stream from EventStore.

How it works:

* Call the [BuildState][0] function, which sends meta-information containing current aggregate-version to EventStoreQuery-service.
* The events will be received on channel returned by above function.

---

### How to use

* Check [**examples**][1] for how to use this library.
* Check [**tests-file**][2] for additional examples.

---

* [Go Docs][3]

---

  [0]: https://godoc.org/github.com/TerrexTech/go-agg-builder/builder#EventsIO.BuildState
  [1]: https://github.com/TerrexTech/go-agg-builder/blob/master/examples/example.go
  [2]: https://github.com/TerrexTech/go-agg-builder/blob/master/builder/builder_suite_test.go
  [3]: https://godoc.org/github.com/TerrexTech/go-agg-builder/builder
