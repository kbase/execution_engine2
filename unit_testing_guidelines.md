# Unit and Integration Testing guidelines

This document briefly covers testing philosophy with regard to integration and unit tests,
especially for the Python language and in the context of developing a KBase core service like
the Execution Engine.

## Unit versus Integration tests

Unit tests cover one module, class, or function, called a code unit from here on out. For example,
a unit test file might cover the contents of `my_module.py` or more granularly `my_module.MyClass`.
Code outside the code unit should be excluded from the tests. The exception is "value classes"
which are classes which primarily hold data and whose behavior is based on that data. Other
classes required by the unit under test should be mocked out as far as possible and practical.

This makes unit tests fast and easy to understand, as only the isolated code unit needs to be
comprehended in order to grasp test failures.

In contrast an integration test tests that two or more code units work well together. This can
range from anything between testing two code units' interactions to api-to-DB tests for a server.
Integration tests are typically much much slower, much more complex, take much more setup code,
and are harder to understand. Due to this, it is advisable to minimze the number of integration
tests to the least possible to ensure the various code units work together correctly, and write
unit tests to cover as much code as possible. In the author's experience, it is usually not
difficult to write unit tests with 100% coverage for the code unit (although keep in mind
that 100% test coverage does not necessarily indicate quality tests).

## Mocking dependencies

As previously described, a unit test should only cover a single unit of code. What this means
is that complex dependencies (e.g. not simple helper functions that may be called from the
class, not value classes, etc.) need to be mocked out. We do this via inversion of control, or
dependency injection. That is, if a code unit needs another code unit as a dependency, the
dependency should *provided to* the code unit rather than *constructed by* the code unit.

For example, consider a toy function that contacts the workspace service:

```
def get_object_name_from_id(url, token, ref):
    ws = Workspace(url, token=token)
    return ws.get_object_info3({'objects': [{'ref': ref}]})['infos'][1]
```

Note that the same situation may arise in a class that needs to contact the workspace regularly and
constructs the client in its `__init__` method.

This makes the function difficult to unit test, as if run as-is, it will contact the workspace
service. This means that to run the test the workspace service must be running and populated
with data, or a mock service must be running that can validate the call and return the expected
payload.

Instead, we can rewrite the function (or class) with dependency injection:

```
def get_object_name_from_id(ws, ref):
    return ws.get_object_info3({'objects': [{'ref': ref}]})['infos'][1]
```

Now we can easily pass in a mock object for the `Workspace` depencency in a unit test:

```
def test_get_object_name_from_id_success():
    ws = create_autospec(Workspace, spec_set=True, instance=True)                    [1]
    ws.get_object_info3.return_value = {'infos': [                                   [2]
        [3,
         'my_name',
         'Some.Type-1.0',
         '1/1/1T01:01:01+00:00',
         1,
         'someguy',
         8,
         'my_workspace',
         '79054025255fb1a26e4bc422aef54eb4', 82, {}]
    ]}

    assert get_object_name_from_id(ws, '8/3/1') == 'my_name'                         [3]

    ws.get_object_info3.assert_called_once_with({'objects': [{'ref': '8/3/1'}]})     [4]
```

In this test, we:
1. Create the mock object
2. Tell the mock object what to return if the `get_object_info3` method is called
3. Call the method with the mock object as an argument and `assert` that it returns the correct
   result
4. Confirm that the mock was called correctly.

No server, mock or otherwise, is required, nor is confusing and error-prone monkey patching.

If step 4 is omitted, any code that is run prior to the mock being called is ignored
by the tests as long as the mock is called and an error doesn't occur. Confirming the correct
call is required to test that any code that, for example, mutates the input arguments before
calling the mock with said mutated arguments works correctly.

For more information on the Python mock standard library, see
https://docs.python.org/3/library/unittest.mock.html.

For an example of mocks used in real code, see
[this EE2 test](https://github.com/kbase/execution_engine2/blob/e2c8086bd1f52b3ca488882c493aaaa9704626ad/test/tests_for_sdkmr/EE2StatusRange_test.py).

## More on Dependency Injection

Dependency Injection (DI), as we've seen, makes unit tests much easier, or even possible. There's
another benefit as well: modularity. DI makes it much easier to swap out modules, even at runtime,
to provide alternate implmentations of the fuctionality. Imagine an application that requires an
authorization module with a large number of parameters:

```
class Application:

    def __init__(self,
            auth_url,
            auth_client_id,
            auth_client_secret,
            auth_protocol,
            auth_cache_time,
            ... more Application parameters go here):
        self.auth = SomeCompaniesAuthImplementation(
            auth_url, auth_client_id, auth_client_secret, auth_protocol, auth_cache_time)
```

If we wish to support `SomeOtherCompaniesAuthImplementation`, determined at runtime, we may need
another batch of parameters to support that implementation as well as a parameter to tell
`Application` which authorization implementation to use.

An implementation based on DI might look like:

```
class Application:

    def __init__(self, auth_implementation):
        self.auth = auth_implementation
```

Where the interface of `auth_implementation` can be merely documented (e.g. ducktyping) or
more rigorously defined with an [abstract base class](https://docs.python.org/3/library/abc.html)
and [type hints](https://docs.python.org/3/library/typing.html).

In this way, code that interprets a configuration at run time can build whichever version of
the authentication module that is required and pass it to `Application`. This makes `Application`
more modular, easier to test, easier to use, and simplifies the initialization.

The drawback of DI is that it pushes the responsibility for building dependencies up the
software stack, making the user of the class have to write that code, although package authors
could provide helper methods.

## `Mock()` versus `create_autospec`

Those familiar with the python mock library will be aware of the `Mock` class. In the examples
above, we use `create_autospec` to create the mock rather than creating a mock class directly.
The way `create_autospec` is used, with `spec_set=True` and `instance=True`, creates a mock object
based off the interface of the class being mocked, and unlike a regular mock, will not allow
reading or writing of an attribute that does not exist on the class being mocked (as well as
avoiding [other problems](https://docs.python.org/3/library/unittest.mock.html#auto-speccing)).
This prevents test false positives if the interface of the class changes but the tests are not
updated - a standard `Mock()` will allow method returns to be set and will record method calls
for methods that do not exist, but in the example above, the tests would fail if, for example,
`get_object_info3` was removed from the `Workspace` class.

The drawback of using `spec_set=True` is that autospeccing is unaware of any instance variables
(e.g. `self.foo = foo_arg` in a constructor, for example). The unittest documentation suggests
a number of approaches to get around this problem, but in the author's opinion the least
bad option is to create getters (and setters for mutable instance variables) for any instance
variables that need to be exposed in the class's public interface. 

## External services

The rule of thumb is to not mock external services, but instead create a wrapper around the
external service, mock that, and test the wrapper with integration tests. In some cases this
is relatively simple, but other cases are much more difficult.

If the service is easy to set up and run locally, an integration test with a live service
is likely the best choice. Databases like MongoDB often fit this category as it is quick to
download and run a binary or Docker image.

If the service is more difficult to run locally, a mock server might be employed to mock the
service responses. This is dangerous because if the service API changes, the test results will
contain false positives. An example is using a mock server in the
[KBase auth2](https://github.com/kbase/auth2) repo to mock identity provider services, which
cannot be installed locally and cannot be incorporated into automated testing without
enormous difficulty.
