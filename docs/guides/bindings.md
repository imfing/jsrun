# Bindings

Bindings let your JavaScript code talk to Python. Think of them as bridges: you expose Python functions and data, and JavaScript can use them naturally.

## Why Use Bindings?

Sometimes you want JavaScript to do the heavy lifting (parsing, transforming data), but you need Python for specific tasks:

- Call a Python API or library
- Access Python data without copying everything
- Let JavaScript trigger Python side effects (logging, notifications, etc.)

Instead of passing data back and forth with `eval()`, you bind once and call many times.

## Binding Functions

Use [`bind_function()`][jsrun.Runtime.bind_function] to expose a Python function to JavaScript:

```python
from jsrun import Runtime

with Runtime() as runtime:
    # Define a Python function
    def greet(name):
        return f"Hello, {name}!"

    # Bind it to JavaScript
    runtime.bind_function("greet", greet)

    # Now JavaScript can call it
    result = runtime.eval("greet('World')")
    print(result)  # "Hello, World!"
```

That's it. JavaScript sees `greet` as a regular function.

### Multiple Arguments

Python functions can accept any number of arguments:

```python
with Runtime() as runtime:
    def add(a, b, c=0):
        return a + b + c

    runtime.bind_function("add", add)

    print(runtime.eval("add(1, 2)"))     # 3
    print(runtime.eval("add(1, 2, 3)"))  # 6
```

Arguments are automatically converted between Python and JavaScript types (numbers, strings, lists, dicts, etc.).

### Async Functions

Async Python functions work too. JavaScript receives a Promise:

```python
import asyncio

async def main():
    with Runtime() as runtime:
        async def fetch_data(url):
            await asyncio.sleep(0.1)  # Simulate async work
            return {"url": url, "status": 200}

        runtime.bind_function("fetchData", fetch_data)

        # JavaScript gets a Promise
        result = await runtime.eval_async("""
            fetchData('https://example.com')
        """)
        print(result)  # {'url': 'https://example.com', 'status': 200}

asyncio.run(main())
```

JavaScript doesn't need to know the function is async, it just awaits the Promise.

## Binding Objects

Use [`bind_object()`][jsrun.Runtime.bind_object] to pass Python data to JavaScript:

```python
with Runtime() as runtime:
    config = {
        "debug": True,
        "timeout": 30,
        "retries": 3
    }

    runtime.bind_object("config", config)

    # JavaScript can read it
    result = runtime.eval("config.debug && config.retries > 0")
    print(result)  # True
```

### What Can You Bind?

You can bind Python values that can be converted to JavaScript:

- **Primitives**: `int`, `float`, `str`, `bool`, `None`
- **Collections**: `list`, `dict`, `tuple`
- **Binary data**: `bytes`, `bytearray`, `memoryview` (becomes `Uint8Array`)

```python
with Runtime() as runtime:
    # Bind various types (must be wrapped in a dict)
    runtime.bind_object("numbers", {"items": [1, 2, 3, 4, 5]})
    runtime.bind_object("user", {"name": "Alice", "age": 30})
    runtime.bind_object("data", {"bytes": b'\x00\x01\x02\x03'})

    # Use them in JavaScript
    runtime.eval("numbers.items.reduce((a, b) => a + b)")  # 15
    runtime.eval("user.name.toUpperCase()")                # "ALICE"
    runtime.eval("data.bytes[0] + data.bytes[1]")          # 1
```

### Objects Are Snapshots

When you bind an object, it gets serialized and JavaScript receives a **copy** of the data at that moment:

```python
with Runtime() as runtime:
    counter = {"value": 0}
    runtime.bind_object("counter", counter)

    # JavaScript modifies its copy
    runtime.eval("counter.value = 10")

    # Python's original is unchanged
    print(counter)  # {'value': 0}
```

If you need shared state, bind a function that returns fresh data each time.

## Practical Examples

### Configuration and Feature Flags

```python
with Runtime() as runtime:
    runtime.bind_object("features", {
        "darkMode": True,
        "experimentalUI": False,
        "maxUploadSize": 10_000_000
    })

    result = runtime.eval("""
        if (features.darkMode) {
            "dark-theme.css"
        } else {
            "light-theme.css"
        }
    """)
    print(result)  # "dark-theme.css"
```

### Logging from JavaScript

```python
with Runtime() as runtime:
    def log(level, message):
        print(f"[{level.upper()}] {message}")

    runtime.bind_function("log", log)

    runtime.eval("""
        log('info', 'Starting process...');
        log('error', 'Something went wrong!');
    """)
    # Output:
    # [INFO] Starting process...
    # [ERROR] Something went wrong!
```

### Data Validation

```python
with Runtime() as runtime:
    def validate_email(email, *args):
        # Accept extra args that JS array methods pass (index, array)
        return "@" in email and "." in email

    runtime.bind_function("validateEmail", validate_email)

    result = runtime.eval("""
        const emails = ['user@example.com', 'invalid', 'test@domain.org'];
        emails.filter(validateEmail)
    """)
    print(result)  # ['user@example.com', 'test@domain.org']
```

### Processing with Python Libraries

```python
with Runtime() as runtime:
    def process_image(data):
        # Imagine using Pillow, OpenCV, etc.
        return f"Processed {len(data)} bytes"

    runtime.bind_function("processImage", process_image)

    # JavaScript sends binary data to Python
    runtime.bind_object("image", {"data": b'\x89PNG\r\n...'})
    result = runtime.eval("processImage(image.data)")
    print(result)  # "Processed 9 bytes"
```

## Decorator Style

For a cleaner syntax, use the `@runtime.bind()` decorator:

```python
import asyncio

async def main():
    with Runtime() as runtime:
        @runtime.bind()
        def calculate(x, y):
            return x * y + 10

        @runtime.bind()
        async def fetch_user(user_id):
            # Simulate async database call
            await asyncio.sleep(0.1)
            return {"id": user_id, "name": "Alice"}

        result = runtime.eval("calculate(5, 3)")
        print(result)  # 25

        user = await runtime.eval_async("fetch_user(123)")
        print(user)  # {'id': 123, 'name': 'Alice'}

asyncio.run(main())
```

The decorator automatically uses the function's name as the binding name in JavaScript. If you want a different name, pass the `name` parameter:

```python
with Runtime() as runtime:
    @runtime.bind(name="add")
    def my_addition_function(a, b):
        return a + b

    result = runtime.eval("add(2, 3)")  # 5
```

## Module-Level API

For quick scripts, use the module-level functions (they use a context-local runtime):

```python
import jsrun

# Bind to the default runtime
jsrun.bind_function("add", lambda a, b: a + b)
jsrun.bind_object("config", {"version": "1.0"})

# Use them immediately
print(jsrun.eval("add(2, 3)"))        # 5
print(jsrun.eval("config.version"))   # "1.0"
```

This is perfect for interactive sessions or simple scripts where you don't need explicit runtime management.

## Tips and Best Practices

**Keep functions simple**: Bound functions should be fast. If you have expensive operations, consider running them in a thread pool and returning a future.

**Bind early**: Set up all your bindings before running complex JavaScript. It's cleaner and easier to debug.

**Use meaningful names**: Make function names clear and follow JavaScript conventions (`camelCase`).

**Don't bind everything**: Only expose what JavaScript actually needs. Keep your API surface small.

**Remember the copy**: Objects are snapshots. For dynamic data, bind a function that returns fresh values.

## Next Steps

- Learn about [Type Conversion](../concepts/types.md) to understand how Python and JavaScript types map
- Explore [Modules](modules.md) to organize code with imports and exports
