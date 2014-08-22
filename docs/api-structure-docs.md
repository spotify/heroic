# API Structure Documentation

## Section

Each endpoint has its own section with a title containing ```<method> <path>```.

Under that title there will be a list, this list contains the applicable response objects that should be expected.

The list contains items of the form ```Response <code> (<content-type>) <type>``` where ```<code>``` is the expected HTTP status code,  ```<content-type>``` is the applicable document type, and ```<type>``` is the structure of the document as described below.

For example.

+ Response 200 (application/json) [Foo](#foo)

This would indicate that the endpoint _could_ return a result with the status code ```200```, that the content type would be ```application/json```, and that the structure of the result would be corresponding to the documented structure of the type ```Foo``` (since this is an example, it does not exist).

## Structure

The structure of a document is defined as follows.

```
<type>:
  <field>: <optional|required> <literal|type|list|map>
  ...
```

```String```, and ```Number``` are built-in types from JSON that are used below.

A ```literal``` refers to JSON literal values, such as the string ```"foo"``` or the number ```12.14```.

A ```<field>``` refers to a key in a JSON object.
The keyword ```optional``` or ```required``` refers to if the field has to be present and non-null or not.

A ```list``` or a ```map``` is characterized with one item which contains their type. It also contains an element ```..``` if more than the specified amount of elements are allowed. Some examples are ```[String, ..]```, ```[String, String]```, and ```{Number: String, ..}```.

The following is an example definition and a corresponding, _valid_ JSON.

```
Foo:
  hello: optional String
  world: required Number
```

```javascript
{"hello": "foo", "world": 12.14}
```
