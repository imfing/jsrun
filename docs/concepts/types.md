# Type Conversion

`jsrun` automatically converts data between Python and JavaScript when you pass values across the language boundary. This makes it seamless to work with both languages without manual conversion.

## Basic Type Mapping

Most common Python types have direct JavaScript equivalents:

| Python              | JavaScript | Notes                                     |
| ------------------- | ---------- | ----------------------------------------- |
| `None`              | `null`     | Python's `None` becomes JavaScript `null` |
| `bool`              | `boolean`  | `True` → `true`, `False` → `false`        |
| `int`               | `number`   | For values within safe integer range      |
| `float`             | `number`   | Including `NaN`, `Infinity`, `-Infinity`  |
| `str`               | `string`   | UTF-8 encoding preserved                  |
| `list`              | `Array`    | Nested lists are converted recursively    |
| `dict`              | `Object`   | Keys must be strings                      |
| `set` / `frozenset` | `Set`      | Order not guaranteed                      |

### Example: Basic Types

```python
from jsrun import Runtime

with Runtime() as runtime:
    # Numbers
    assert runtime.eval("2 + 2") == 4
    assert runtime.eval("Math.PI") == 3.141592653589793

    # Strings
    assert runtime.eval("'hello'.toUpperCase()") == "HELLO"

    # Booleans
    assert runtime.eval("true && false") == False

    # null and None
    assert runtime.eval("null") is None

    # Arrays and lists
    assert runtime.eval("[1, 2, 3]") == [1, 2, 3]

    # Objects and dicts
    result = runtime.eval("({name: 'Alice', age: 30})")
    assert result == {"name": "Alice", "age": 30}
```

## Special Types

### Undefined

JavaScript has both [`null`][javascript-null] and [`undefined`][javascript-undefined], but Python only has `None`. To distinguish them, `jsrun` provides a special sentinel value [`undefined`][jsrun.undefined]:

```python
import jsrun
from jsrun import undefined

# Pass undefined to JavaScript
jsrun.eval("let x")  # x is undefined
result = jsrun.eval("x")
assert result is undefined

# Check if a value is undefined
if result is undefined:
    print("Value is undefined")
```

When JavaScript returns `undefined`, you'll receive [`undefined`][jsrun.undefined] in Python.
When JavaScript returns `null`, you'll receive Python's `None`.

### BigInt

JavaScript [`BigInt`][javascript-bigint] values convert to Python's arbitrary-precision integers:

```python
with Runtime() as runtime:
    # JavaScript BigInt → Python int
    big = runtime.eval("9007199254740991n")  # JavaScript BigInt literal
    assert isinstance(big, int)
    assert big == 9007199254740991

    # Python int → JavaScript BigInt (for large numbers)
    runtime.eval(f"const x = {2**100}n")
    result = runtime.eval("x * 2n")
    assert result == 2**101
```

Small integers (within JavaScript's safe integer range: -2^53 to 2^53) are represented as regular JavaScript numbers. Larger integers automatically become BigInt values in JavaScript.

### Date and Datetime

Python's [`datetime`][python-datetime] objects convert to JavaScript [`Date`][javascript-date] objects and vice versa:

```python
from datetime import datetime, timezone

with Runtime() as runtime:
    # JavaScript Date → Python datetime
    js_date = runtime.eval("new Date('2024-01-15T12:30:00Z')")
    assert isinstance(js_date, datetime)
    assert js_date.tzinfo == timezone.utc

    # Python datetime → JavaScript Date
    now = datetime.now(timezone.utc)
    runtime.eval(f"const pyDate = new Date({int(now.timestamp() * 1000)})")
    result = runtime.eval("pyDate.getFullYear()")
    assert result == now.year
```

All datetime values are normalized to UTC during conversion. If you pass a naive (timezone-unaware) Python datetime, it will be treated as UTC.

### Temporal

JavaScript's [Temporal][temporal] API (available natively in the bundled V8 engine) converts to Python's native date/time types, and Python `date`, `time`, and `timedelta` values convert to Temporal instances:

| JavaScript | Python | Notes |
|---|---|---|
| `Temporal.Instant` | aware `datetime` (UTC) | sub-microsecond precision truncated |
| `Temporal.ZonedDateTime` | aware `datetime` (`ZoneInfo` or fixed offset) | |
| `Temporal.PlainDate` | `date` | non-ISO calendars normalized to ISO |
| `Temporal.PlainTime` | `time` | |
| `Temporal.PlainDateTime` | naive `datetime` | |
| `Temporal.Duration` | `timedelta` | |

```python
from datetime import date, timedelta

with Runtime() as runtime:
    # Temporal → Python
    d = runtime.eval("Temporal.PlainDate.from('2026-10-03')")
    assert d == date(2026, 10, 3)

    # Python → Temporal
    check = runtime.eval("(d) => d instanceof Temporal.PlainDate")
    assert check(date(2026, 10, 3)) is True
```

Two asymmetries to be aware of: Python `datetime` still converts to a JavaScript `Date` (not Temporal) for backwards compatibility, and `Temporal.Duration` values with nonzero `years`/`months`/`weeks` are calendar-relative with no fixed length, so they are not converted to `timedelta`.

[temporal]: https://tc39.es/proposal-temporal/docs/

## Binary Data

Binary data is represented as `bytes` in Python and `Uint8Array` in JavaScript:

```python
with Runtime() as runtime:
    # JavaScript Uint8Array → Python bytes
    binary = runtime.eval("new Uint8Array([72, 101, 108, 108, 111])")
    assert binary == b'Hello'

    # Python bytes → JavaScript Uint8Array (via bind_object)
    data = b'\x00\x01\x02\x03'
    runtime.bind_object("raw", {"data": data })
    result = runtime.eval("raw.data[0] + raw.data[1]")  # 1
```

## Collections

### Arrays and Lists

Python lists become JavaScript arrays, preserving order and allowing mixed types:

```python
with Runtime() as runtime:
    # Nested structures
    nested = runtime.eval("[[1, 2], [3, 4], [5, 6]]")
    assert nested == [[1, 2], [3, 4], [5, 6]]

    # Mixed types
    mixed = runtime.eval("[1, 'two', true, null]")
    assert mixed == [1, "two", True, None]
```

### Objects and Dicts

Python dictionaries with string keys become JavaScript objects:

```python
with Runtime() as runtime:
    obj = runtime.eval("""({
        name: 'Product',
        price: 99.99,
        tags: ['new', 'sale'],
        metadata: { color: 'blue' }
    })""")

    assert obj["name"] == "Product"
    assert obj["price"] == 99.99
    assert obj["tags"] == ["new", "sale"]
    assert obj["metadata"]["color"] == "blue"
```

!!! info
    Only **string keys** are supported. Attempting to convert a Python dict with non-string keys will raise an error.

```python
# This will raise an error
bad_dict = {1: "one", 2: "two"}  # Integer keys not supported
```

### Sets

Python sets convert to JavaScript `Set` objects:

```python
with Runtime() as runtime:
    # JavaScript Set → Python set
    js_set = runtime.eval("new Set([1, 2, 3, 2, 1])")
    assert js_set == {1, 2, 3}
```

Note that JavaScript `Set` does not guarantee iteration order, though modern JavaScript engines preserve insertion order.

## Special Values

### NaN and Infinity

JavaScript's special numeric values are preserved:

```python
with Runtime() as runtime:
    import math

    # NaN
    nan = runtime.eval("NaN")
    assert math.isnan(nan)

    # Infinity
    inf = runtime.eval("Infinity")
    assert math.isinf(inf) and inf > 0

    # Negative infinity
    neg_inf = runtime.eval("-Infinity")
    assert math.isinf(neg_inf) and neg_inf < 0
```

## Unsupported Types

Some Python and JavaScript types cannot be automatically converted:

**Python → JavaScript**:

- `tuple` (convert to `list` first)
- Custom classes and objects (unless using ops or bindings)
- Functions (use function binding instead)

**JavaScript → Python**:

- `Symbol`
- `Map` and `WeakMap` (use objects or bindings instead)
- `Function` objects (proxied via special binding)

If you need to pass unsupported types, consider:

- Using function or object binding (see [Bindings guide](../guides/bindings.md))
- Converting to supported types before passing

## Conversion Limits

To prevent excessive memory usage, `jsrun` enforces default limits on data conversion:

- **Maximum depth**: 64 levels of nesting (arrays/objects)
- **Maximum size**: 10 MB total for a single value

These limits apply when passing data across the language boundary. Exceeding them raises a `RuntimeError`.
You can customize these limits when creating a runtime (see [`RuntimeConfig`][jsrun.RuntimeConfig]).

## Performance Considerations

Type conversion has some overhead. For best performance:

1. **Minimize boundary crossings**: Prefer doing work entirely in JavaScript or Python rather than passing data back and forth
2. **Avoid large objects**: Converting megabytes of nested data can be slow

## Type Safety

Python and JavaScript have different type systems. Keep these differences in mind:

- **JavaScript is loosely typed**: `"5" + 5` equals `"55"` in JavaScript
- **No integer/float distinction in JavaScript**: All numbers are floats (except BigInt)
- **Truthy/falsy values differ**: Empty arrays are truthy in JavaScript but would be falsy in Python boolean contexts
- **Property access**: JavaScript allows `undefined` for missing properties; Python raises `KeyError`

Always validate and sanitize data at the boundary:

```python
from jsrun import Runtime, undefined

with Runtime() as runtime:
    result = runtime.eval("({missing: undefined})")

    # Safe: Check before accessing
    if "value" in result:
        print(result["value"])

    # Safe: Handle undefined
    if result.get("missing") is undefined:
        print("Property not set")
```

[javascript-undefined]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/undefined
[javascript-null]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Operators/null
[javascript-bigint]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/BigInt
[javascript-date]: https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Date
[python-datetime]: https://docs.python.org/3/library/datetime.html
