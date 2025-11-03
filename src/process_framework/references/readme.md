# References

A `Reference` is a boxed reference to a value of a known type.

`Reference`s perform runtime type-checking.

The value assigned to a `Reference` must be assignable to its `_type`.

An optional `on_set` callback can is invoked when a value is assigned.

`Reference` implements a few methods to aid in generating a meaningful string representation; these attempt to handle `pandas` `DataFrame` and `Series` values without introducing a dependency to that package.

`Reference` provides the following public methods:
* `set`:        assign a value to the `Reference`; throw an error if the value is not `None` and is not assignable to the `Reference`'s `_type`.
* `is_instance_of`: wraps `isinstance`; returns `True` if the `Reference`'s `value` can be assigned to the provided `class_or_tuple`.
* `has_value`:  returns `True` if `value` is not `None`, else `False`.
* `get_value`:  throws an error if `value` is `None`, else returns the `value`.

## See also `references.dataframe`
