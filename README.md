# UDP-based Request-Reply Bridge for Erlang nodes.

## Summary

Provides an API to make Request-Reply interactions between
an Erlang nodes using the UDP/IP network protocol.

## Main features

* fully asynchronous;
* symmetric (each node with Bridge started can call any other node);
* one bridge can serve requests from many local Erlang processes;
* does not consume file descriptors;
* does not any complex data processing, as it operates only
 with binaries (the user must implement payload encoding and
 deconding by himself).

## Example with message passing

On the first node:

```
{ok, Pid} = udpcall:start_link([{bind_port, 5000}, {receiver, self()}]),
EncodedRequest = term_to_binary(5),
{ok, EncodedReply} =
    udpcall:call(Pid, "node2.com", 5001, EncodedRequest, 1000),
10 = binary_to_term(EncodedReply),
...
```

On the second node:

```
{ok, Pid} = udpcall:start_link([{bind_port, 5001}, {receiver, self()}]),
receive
    {rcall, Pid, Ref, EncodedRequest} ->
        Request = binary_to_term(EncodedRequest),
        Reply = Request * 2,
        EncodedReply = term_to_binary(Reply),
        ok = udpcall:reply(Pid, Ref, EncodedReply),
        ...
```

## Example with callback function

On the first node:

```
{ok, Pid} = udpcall:start_link([{bind_port, 5000}, {receiver, self()}]),
EncodedRequest = term_to_binary(5),
{ok, EncodedReply} =
    udpcall:call(Pid, "node2.com", 5001, EncodedRequest, 1000),
10 = binary_to_term(EncodedReply),
...
```

On the second node:

```
{ok, Pid} =
    udpcall:start_link(
        [{bind_port, 5001},
         {receiver,
          fun(EncodedRequest) ->
              Request = binary_to_term(EncodedRequest),
              Reply = Request * 2,
              term_to_binary(Reply)
          end}]),
```
