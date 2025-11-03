# Steps

A `Step` is a stateless unit of work.

Values required by or assigned to by the unity of work are passed as `Reference`s.

`Step`s have a constructor, accepting `Reference`s and other arguments, and a single `do` method, which performs the unit of work.

The `process_framework` provides specialised `Step`s for performing common data transformation, elasticsearch indexing, geospatial and sql operations.

Most `Step`s will derive from one of a handful of derived classes:
* `AssigningStep`:      a step that takes no input, generates a value and assigns a typed result to a typed `Reference` 

* `ModifyingStep`:      a step that applies a `transform` to the provided `subject` and returns a result of the same type. If `assign_to` is provided when the `Step` is constructed, the result is assigned to it, otherwise result is assigned 'in place' to the `subject`.

* `TransformingStep`:   a step that takes a `subject` of one type and assigns a value to an `assign_to` reference of another, implements a `transform` `abstractmethod` which transforms the value of the `subject` reference into an instance of the type of the `assign_to` reference.