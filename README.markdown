Overview
--------
riakpool is an application for maintaining a dynamic pool of protocol buffer
client connections to a riak database. It ensures that a given connection can
only be in use by one external process at a time and is based on the design created by David Weldon.

This version contains the following changes/enhancements:

- execute/1 and the functions in the client library now return *'any()'*
      instead of *'{ok, any()}'* for successful operations.
      
- A target pool_size can be specified. The pool can grow beyond this size
      but will shrink back to this target size once sufficient connections have
      been idle longer than the release_delay parameter specifies.
      
- An increment parameter has been introduced to allow the pool to expand
      quicker when insufficient connections are available in order to better respond 
      to spikes.
      
- All connections have a max age. Once they are older than this max age, the
      connection will be replaced with a new one. If a load balancer like e.g.
      HAProxy is used on the node where the pool resides, this will allow the
      pool to rebalance and restore connections following a failure. 

Installation
------------
    $ git clone git@github.com:whitenode/riakpool.git
    $ cd riakpool
    $ make all

Interface
---------
The following example gives an overview of the riakpool interface. Please see
the complete documentation by running *make doc*.

    1> application:start(riakpool).
    ok
    2> riakpool:start_pool("127.0.0.1", 8087, [{pool_size, 1}]).
    ok
    3> riakpool:execute(fun(C) -> riakc_pb_socket:ping(C) end).
    pong
    4> riakpool_client:put(<<"groceries">>, <<"mine">>, <<"eggs">>).
    ok
    5> riakpool_client:get(<<"groceries">>, <<"mine">>).
    <<"eggs">>
    6> riakpool_client:list_keys(<<"groceries">>).
    [<<"mine">>]
    7> riakpool_client:delete(<<"groceries">>, <<"mine">>).
    ok
    8> riakpool:count().
    1

Note that the use of riakpool_client is completely optional - it is simply a
collection of convenience functions which call *riakpool:execute/1*.

Starting the Pool
-----------------
Prior to any calls to *riakpool:execute/1*, the pool must be started. This can
be accomplished in one of two ways:

1. Before the server is started, set the riakpool application environment
   variables **riakpool_host** and **riakpool_port**. This will start the pool with
   default settings. The following parameters can be specified to alter the
   default settings:
   
    **pool_size** - Target size of connection pool. [default: 5]
    
    **increment** - When the size of the pool needs to be extended, this is
                    the number of connections that will be added. [default: 1]
                     
    **max_age** - Indicates the max age of a connection in seconds. Once a
                  connection exceeds this age it will be replaced with a new
                  one. [default: 600]
                     
    **release_delay** - Once a connection has been idle for this amount of time in
                        seconds it will be considered for removal if the number of
                        current connections exceed the target pool size. [default: 10]
    
2. After the application is started, call *riakpool:start_pool/0*, *riakpool:start_pool/2*
   or *riakpool:start_pool/3* (see previous section). If *riakpool:start_pool/3* is
   used, the parameters specified above can be passed to override
   default settings.
