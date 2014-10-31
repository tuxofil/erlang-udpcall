%%% @doc
%%% UDP Request-Reply Bridge main interface module.
%%%
%%% The bridge does not any encoding/decoding of the payload
%%% data and assumes request and reply is given as binaries.
%%% This is done to reduce memory copies of a potentially big
%%% Erlang terms between processes.
%%%
%%% If receiver process is not defined, spawned UDP bridge
%%% can be used only for sending requests to a remote side
%%% and all requests, received from the remote side, will
%%% be silently dropped.
%%%
%%% If receiver process is defined (with {receiver, atom() | pid()}
%%% option for the start_link/1 fun), when a remote side makes
%%% request, it will be delivered to the receiver process as:
%%%
%%% <tt>{rcall, BridgeRef :: atom() | pid(), From :: any(),
%%%   Data :: binary()}</tt>
%%%
%%% Where BridgeRef is a term identifying the bridge received the
%%% request, From is a opaque value, identifying the request,
%%% Data is a request itself.
%%%
%%% Once the request is processed, the receiver process may
%%% send a reply with reply/2 function, passing From term as
%%% the first argument. The reply/2 call can be applied from
%%% context of any process, not the only process which received
%%% the rcall-request.
%%%
%%% Dataflow diagram is as follows:
%%% <ol>
%%%  <li>Process P1 on Node1 calls blocking
%%%    <tt>call(BridgeRef, Host, Port, Request, Timeout)</tt>;</li>
%%%  <li>Request is encoded, transferred to the remote
%%%    side (Node2) and dispatched to the receiver process (P2)
%%%    as Erlang message
%%%    <tt>{rcall, BridgeRef, From, Request}</tt>;</li>
%%%  <li>Receiver process (P2) on the Node2 fetches the message
%%%    from the mailbox, process it, and calls non-blocking
%%%    <tt>reply(BridgeRef, From, Reply)</tt>;</li>
%%%  <li>The reply is encoded, transferred to the Node1,
%%%    dispatched to the P1 process and returned as result
%%%    of the call/5 function call.</li>
%%% </ol>
%%%
%%% When receiver is defined as {Mod, Fun} tuple or as
%%% functional object of arity 1, no rcall-tagged message will
%%% be sent instead the new process will be spawned automatically
%%% and the function will be applied with Request as the only
%%% argument. The function must return binary, which will be
%%% sent back to the caller process.
%%%
%%% In this case dataflow diagram is as follows:
%%% <ol>
%%%  <li>Process P1 on Node1 calls blocking
%%%    <tt>call(BridgeRef, Host, Port, Request, Timeout)</tt>;</li>
%%%  <li>Request is encoded, transferred to the remote
%%%    side (Node2), a new process (P2) spawned;</li>
%%%  <li>The new process (P2) on the Node2 applies
%%%    functional object, described in the 'receiver' option
%%%    with the Request as the only argument;</li>
%%%  <li>The return value of the functional object is encoded,
%%%     transferred to the Node1, dispatched to the P1 process
%%%     and returned as result of the call/5 function call.</li>
%%% </ol>

%%% @author Aleksey Morarash <aleksey.morarash@gmail.com>
%%% @since 31 Oct 2014
%%% @copyright 2014, Aleksey Morarash <aleksey.morarash@gmail.com>

-module(udpcall).

%% API exports
-export(
   [start_link/1,
    call/5,
    reply/3,
    stop/1
   ]).

%% gen_server callback exports
-export(
   [init/1, handle_call/3, handle_info/2, handle_cast/2,
    terminate/2, code_change/3]).

-include("udpcall.hrl").

%% --------------------------------------------------------------------
%% Data type definitions
%% --------------------------------------------------------------------

-export_type(
   [options/0,
    receiver/0,
    bridge_ref/0,
    host/0,
    data/0,
    rreq_ref/0
   ]).

-type options() :: [option()].

-type option() ::
        {bind_port, inet:port_number()} |
        {receiver, receiver()} |
        {name, atom()}.

-type receiver() ::
        (ProcessID :: pid()) |
        (ProcessRegisteredName :: atom()) |
        (Callback :: {Module :: atom(), Function :: atom()} |
                     fun((Request :: data()) -> Reply :: data())).

-type bridge_ref() :: pid() | atom().

-type host() :: inet:hostname() | inet:ip_address().

-type data() :: binary().

-type socket() :: port().

-type cookie() :: 0..16#ffffffff.

-type expire_time() :: micros() | 0.

-type micros() :: pos_integer().

-type lreq_ref() :: reference().

-type peername() ::
        {inet:ip_address(), inet:port_number()}.

-opaque rreq_ref() :: reference().

%% --------------------------------------------------------------------
%% API functions
%% --------------------------------------------------------------------

%% @doc Start UDP bridge as part of the supervision tree.
-spec start_link(Options :: options()) ->
                        {ok, Pid :: pid()} | ignore |
                        {error, Reason :: any()}.
start_link(Options) ->
    gen_server:start_link(?MODULE, Options, _GenServerOptions = []).

%% @doc Make a Request-Reply interaction with a remote server.
-spec call(BridgeRef :: bridge_ref(),
           Host :: host(), Port :: inet:port_number(),
           Request :: data(),
           Timeout :: timeout()) ->
                  {ok, Reply :: any()} |
                  {error, Reason :: any()}.
call(BridgeRef, Host, Port, Request, Timeout)
  when is_binary(Request) ->
    Ref = make_ref(),
    ExpireTime = timeout_to_expire_time(Timeout),
    _Sent = BridgeRef ! ?LCALL(self(), Ref, Host, Port, Request, ExpireTime),
    receive
        ?LRESP(Ref, {ok, _Reply} = Ok) ->
            Ok;
        ?LRESP(Ref, {error, _Reason} = Error) ->
            Error
    after Timeout ->
            {error, timeout}
    end.

%% @doc Reply to a request from a remote side.
-spec reply(BridgeRef :: bridge_ref(),
            To :: rreq_ref(), Reply :: data()) -> ok.
reply(BridgeRef, To, Reply) when is_binary(Reply) ->
    _Sent = BridgeRef ! ?RRESP(To, Reply),
    ok.

%% @hidden
%% @doc Tell the bridge process to stop.
%% It is not a part of public API.
-spec stop(BridgeRef :: bridge_ref()) -> ok.
stop(BridgeRef) ->
    _Sent = BridgeRef ! stop,
    ok.

%% --------------------------------------------------------------------
%% gen_server callback functions
%% --------------------------------------------------------------------

-record(state, {socket :: socket(),
                receiver :: receiver() | undefined,
                bridge_ref :: bridge_ref(),
                conntrack_l :: ets:tab(),
                conntrack_r :: ets:tab()}).

%% @hidden
-spec init(Options :: options()) -> {ok, InitialState :: #state{}}.
init(Options) ->
    BridgeRef =
        case lists:keyfind(name, 1, Options) of
            {name, Name} ->
                true = register(Name, self()),
                Name;
            false ->
                self()
        end,
    UdpOpts = [{active, true}, {reuseaddr, true}, binary],
    BindPort = proplists:get_value(bind_port, Options, 0),
    {ok, Socket} = gen_udp:open(BindPort, UdpOpts),
    Receiver =
        case lists:keyfind(receiver, 1, Options) of
            {receiver, ReceiverValue}
              when is_atom(ReceiverValue);
                   is_pid(ReceiverValue);
                   is_function(ReceiverValue, 1) ->
                ReceiverValue;
            {receiver, {Module, Function} = ReceiverValue}
              when is_atom(Module), is_atom(Function) ->
                ReceiverValue;
            false ->
                undefined
        end,
    _OldSeed = random:seed(now()),
    %% create connection tracking tables:
    ConnTrackL = ets:new(?MODULE, [public, {keypos, 2}]),
    ConnTrackR = ets:new(?MODULE, [public, {keypos, 2}]),
    %% schedule connection tracking tables clean
    {ok, _TRef} =
        timer:apply_interval(
          _VacuumInterval = 30 * 1000,
          erlang, apply, [fun vacuum/2, [ConnTrackL, ConnTrackR]]),
    {ok, _State = #state{socket = Socket,
                         receiver = Receiver,
                         bridge_ref = BridgeRef,
                         conntrack_l = ConnTrackL,
                         conntrack_r = ConnTrackR
                        }}.

%% @hidden
-spec handle_cast(Request :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_cast(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_info(stop, State :: #state{}) ->
                         {stop, normal, #state{}};
                 ({udp, socket(),
                   inet:ip_address(), inet:port_number(),
                   data()}, #state{}) ->
                         {noreply, State :: #state{}};
                 (?LCALL(pid(), reference(), host(), inet:port_number(),
                         data(), expire_time()), #state{}) ->
                         {noreply, State :: #state{}};
                 (?RRESP(reference(), data()), #state{}) ->
                         {noreply, State :: #state{}}.
handle_info({udp, Socket, FromIP, FromPort, Packet}, State)
  when Socket == State#state.socket ->
    %% Received an UDP packet from a remote side.
    case Packet of
        ?REQ_PACKET(Cookie, ExpireTime, Request)
          when State#state.receiver /= undefined ->
            %% Request message from the remote side.
            ok = handle_req_from_remote(
                   State#state.bridge_ref,
                   State#state.conntrack_r,
                   Cookie, ExpireTime, FromIP, FromPort,
                   Request, State#state.receiver);
        ?REP_PACKET(Cookie, Reply) ->
            %% Reply message from the remote side.
            ok = handle_rep_from_remote(
                   State#state.conntrack_l, Cookie, Reply);
        ?REP_CRASH_PACKET(Cookie, Reason) ->
            %% Reply message from the remote side.
            ok = handle_rep_crash_from_remote(
                   State#state.conntrack_l, Cookie, Reason);
        _Other ->
            %% ignore
            ok
    end,
    {noreply, State};
handle_info(?LCALL(From, Ref, ToHost, ToPort, Request, ExpireTime), State) ->
    %% Received Request message (to remote side)
    %% from a local Erlang process.
    ok = handle_req_from_local(
           State#state.socket, State#state.conntrack_l,
           From, Ref, ToHost, ToPort,
           Request, ExpireTime),
    {noreply, State};
handle_info(?RRESP(RReqRef, Reply), State) ->
    %% Received reply message (from local process to a remote side).
    ok = handle_rep_from_local(
           State#state.socket, State#state.conntrack_r, RReqRef, Reply),
    {noreply, State};
handle_info(?RRESP_CRASH(RReqRef, Reason), State) ->
    %% Received crash report (from local process to a remote side).
    ok = handle_rep_crash_from_local(
           State#state.socket, State#state.conntrack_r, RReqRef, Reason),
    {noreply, State};
handle_info(stop, State) ->
    {stop, _Reason = normal, State};
handle_info(_Request, State) ->
    {noreply, State}.

%% @hidden
-spec handle_call(Request :: any(), From :: any(), State :: #state{}) ->
                         {noreply, NewState :: #state{}}.
handle_call(_Request, _From, State) ->
    {noreply, State}.

%% @hidden
-spec terminate(Reason :: any(), State :: #state{}) -> ok.
terminate(_Reason, _State) ->
    ok.

%% @hidden
-spec code_change(OldVersion :: any(), State :: #state{}, Extra :: any()) ->
                         {ok, NewState :: #state{}}.
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ----------------------------------------------------------------------
%% Internal functions
%% ----------------------------------------------------------------------

%% @doc Convert timeout value to a constant expiration time.
-spec timeout_to_expire_time(timeout()) -> expire_time().
timeout_to_expire_time(infinity) ->
    0;
timeout_to_expire_time(Millis) ->
    micros() + Millis * 1000.

%% @doc Check if the time is elapsed or not.
-spec is_expired(expire_time()) -> boolean().
is_expired(0) ->
    false;
is_expired(Micros) ->
    micros() > Micros.

%% @doc Return micros elapsed since Unix Epoch.
-spec micros() -> micros().
micros() ->
    {MegaSeconds, Seconds, MicroSeconds} = os:timestamp(),
    (MegaSeconds * 1000000 + Seconds) * 1000000 + MicroSeconds.

%% @doc The function handles request message to a local Erlang
%% process, received from a remote side over the UDP/IP.
-spec handle_req_from_remote(bridge_ref(),
                             ConnTrackRemote :: ets:tab(),
                             cookie(), expire_time(),
                             inet:ip_address(), inet:port_number(),
                             data(), receiver()) -> ok.
handle_req_from_remote(BridgeRef, ConnTrackRemote, Cookie, ExpireTime,
                       FromIP, FromPort, Request, Receiver) ->
    case is_expired(ExpireTime) of
        true ->
            %% already expired. Ignore
            ok;
        false ->
            RReqRef = make_ref(),
            %% register the request
            true =
                ets:insert(
                  ConnTrackRemote,
                  #rconntrack{ref = RReqRef,
                              cookie = Cookie,
                              peername = {FromIP, FromPort},
                              expires = ExpireTime}),
            send_to_receiver(BridgeRef, RReqRef, Receiver, Request)
    end.

%% @doc Safely send a message to the receiver process,
%% if it is alive or spawn request processing if receiver
%% is defined as MFA of functional object.
-spec send_to_receiver(BridgeRef :: bridge_ref(),
                       RReqRef :: reference(),
                       receiver(),
                       Request :: data()) -> ok.
send_to_receiver(BridgeRef, RReqRef, Receiver, Request)
  when is_atom(Receiver) ->
    case whereis(Receiver) of
        Pid when is_pid(Pid) ->
            send_to_receiver(BridgeRef, RReqRef, Pid, Request);
        _NotAPid ->
            %% receiver is not alive at the moment
            ok
    end;
send_to_receiver(BridgeRef, RReqRef, {Module, Function}, Request) ->
    send_to_receiver(
      BridgeRef, RReqRef,
      fun(Req) -> Module:Function(Req) end,
      Request);
send_to_receiver(_BridgeRef, RReqRef, Fun, Request)
  when is_function(Fun, 1) ->
    BridgePid = self(),
    %% Spawn the request processing in a separate process,
    %% which will deliver reply to the bridge.
    _Pid =
        spawn(
          fun() ->
                  try
                      Reply = Fun(Request),
                      _Sent = BridgePid ! ?RRESP(RReqRef, Reply),
                      ok
                  catch
                      Type:Reason ->
                          FinalReason =
                              {crash_report,
                               [{type, Type},
                                {reason, Reason},
                                {stacktrace,
                                 erlang:get_stacktrace()}]},
                          Msg = ?RRESP_CRASH(RReqRef, FinalReason),
                          _Sent2 = BridgePid ! Msg,
                          ok
                  end
          end),
    ok;
send_to_receiver(BridgeRef, RReqRef, Pid, Request) ->
    _Sent = Pid ! ?RCALL(BridgeRef, RReqRef, Request),
    ok.

%% @doc The function handles reply message for a local Erlang
%% process, received from a remote side over the UDP/IP.
-spec handle_rep_from_remote(ConnTrackLocal :: ets:tab(),
                             cookie(), data()) -> ok.
handle_rep_from_remote(ConnTrackLocal, Cookie, Reply) ->
    case ets:lookup(ConnTrackLocal, Cookie) of
        [#lconntrack{ref = Ref,
                     sender = Requester}] ->
            %% unregister request
            true = ets:delete(ConnTrackLocal, Cookie),
            _Sent = Requester ! ?LRESP(Ref, {ok, Reply}),
            ok;
        _ ->
            %% No such connection in conntrack table.
            %% Ignore it.
            ok
    end.

%% @doc The function handles report for a crash occured
%% while processing request from a local Erlang process
%% on the remote side.
-spec handle_rep_crash_from_remote(
        ConnTrackLocal :: ets:tab(),
        cookie(),
        EncodedReason :: data()) -> ok.
handle_rep_crash_from_remote(ConnTrackLocal, Cookie, EncodedReason) ->
    case ets:lookup(ConnTrackLocal, Cookie) of
        [#lconntrack{ref = Ref,
                     sender = Requester}] ->
            %% unregister request
            true = ets:delete(ConnTrackLocal, Cookie),
            Reason = binary_to_term(EncodedReason),
            _Sent = Requester ! ?LRESP(Ref, {error, Reason}),
            ok;
        _ ->
            %% No such connection in conntrack table.
            %% Ignore it.
            ok
    end.

%% @doc The function handles request message, sent from a local
%% Erlang process with the call/5 API call.
-spec handle_req_from_local(
        Socket :: socket(),
        ConnTrackLocal :: ets:tab(),
        From :: pid(),
        Ref :: lreq_ref(),
        ToHost :: host(),
        ToPort :: inet:port_number(),
        Request :: data(),
        ExpireTime :: expire_time()) -> ok.
handle_req_from_local(Socket, ConnTrackLocal, From, Ref,
                      ToHost, ToPort, Request, ExpireTime) ->
    case is_expired(ExpireTime) of
        true ->
            %% Already expired. Just ignore.
            %% This is the case when the process message queue is
            %% overloaded with incoming requests.
            ok;
        false ->
            Cookie = random:uniform(16#ffffffff),
            case gen_udp:send(
                   Socket, ToHost, ToPort,
                   ?REQ_PACKET(Cookie, ExpireTime, Request)) of
                ok ->
                    true =
                        ets:insert(
                          ConnTrackLocal,
                          #lconntrack{cookie = Cookie,
                                      ref = Ref,
                                      sender = From,
                                      expires = ExpireTime}),
                    ok;
                {error, _Reason} = Error ->
                    %% reply immediately with error reason
                    _Sent = From ! ?LRESP(Ref, Error),
                    ok
            end
    end.

%% @doc The function handles reply, received from a local
%% Erlang process, sent with reply/3 API call.
-spec handle_rep_from_local(Socket :: socket(),
                            ConnTrackRemote :: ets:tab(),
                            RReqRef :: rreq_ref(), Reply :: data()) -> ok.
handle_rep_from_local(Socket, ConnTrackRemote, RReqRef, Reply) ->
    case ets:lookup(ConnTrackRemote, RReqRef) of
        [#rconntrack{cookie = Cookie,
                     peername = {ToHost, ToPort}}] ->
            %% unregister request
            true = ets:delete(ConnTrackRemote, RReqRef),
            %% Ignore errors, this is not our businnes.
            _Ignore = gen_udp:send(
                        Socket, ToHost, ToPort,
                        ?REP_PACKET(Cookie, Reply)),
            ok;
        _ ->
            %% No such connection in conntrack table.
            %% Ignore it.
            ok
    end.

%% @doc The function handles crash report for error occured
%% during processing a request from a remote side (when Receiver
%% is defined as functional object).
-spec handle_rep_crash_from_local(
        Socket :: socket(),
        ConnTrackRemote :: ets:tab(),
        RReqRef :: rreq_ref(),
        Reason :: any()) -> ok.
handle_rep_crash_from_local(Socket, ConnTrackRemote, RReqRef, Reason) ->
    case ets:lookup(ConnTrackRemote, RReqRef) of
        [#rconntrack{cookie = Cookie,
                     peername = {ToHost, ToPort}}] ->
            %% unregister request
            true = ets:delete(ConnTrackRemote, RReqRef),
            EncodedReason = term_to_binary(Reason),
            %% Ignore errors, this is not our businnes.
            _Ignore = gen_udp:send(
                        Socket, ToHost, ToPort,
                        ?REP_CRASH_PACKET(Cookie, EncodedReason)),
            ok;
        _ ->
            %% No such connection in conntrack table.
            %% Ignore it.
            ok
    end.

%% @doc Remove all expired records from connection tracking tables.
-spec vacuum(ConnTrackLocal :: ets:tab(),
             ConnTrackRemote :: ets:tab()) -> ok.
vacuum(ConnTrackLocal, ConnTrackRemote) ->
    Now = micros(),
    undefined =
        ets:foldl(
          fun(#lconntrack{expires = 0}, Accum) ->
                  Accum;
             (#lconntrack{expires = ExpireTime,
                          cookie = Cookie}, Accum)
                when ExpireTime < Now ->
                  true = ets:delete(ConnTrackLocal, Cookie),
                  Accum;
             (_NotExpired, Accum) ->
                  Accum
          end, undefined, ConnTrackLocal),
    undefined =
        ets:foldl(
          fun(#rconntrack{expires = 0}, Accum) ->
                  Accum;
             (#rconntrack{expires = ExpireTime,
                          ref = RReqRef}, Accum)
                when ExpireTime < Now ->
                  true = ets:delete(ConnTrackRemote, RReqRef),
                  Accum;
             (_NotExpired, Accum) ->
                  Accum
          end, undefined, ConnTrackRemote),
    ok.

%% ----------------------------------------------------------------------
%% EUnit tests
%% ----------------------------------------------------------------------

-ifdef(TEST).

start_stop_test_() ->
    {setup,
     _Setup =
         fun() ->
                 {ok, _Pid1} =
                     start_link([{bind_port, 5000},
                                 {receiver,
                                  fun(T) ->
                                          term_to_binary(
                                            binary_to_term(T) * 2)
                                  end},
                                 {name, b1}]),
                 {ok, _Pid2} =
                     start_link([{bind_port, 5001},
                                 {receiver,
                                  fun(T) ->
                                          term_to_binary(
                                            binary_to_term(T) * 4)
                                  end},
                                 {name, b2}]),
                 %% slowpoke
                 {ok, _Pid3} =
                     start_link([{bind_port, 5002},
                                 {receiver,
                                  fun(T) ->
                                          ok = timer:sleep(1000),
                                          T
                                  end},
                                 {name, b3}]),
                 {b1, b2, b3}
         end,
     _CleanUp =
         fun({B1, B2, B3}) ->
                 ok = stop(B1),
                 ok = stop(B2),
                 ok = stop(B3)
         end,
     [{"B1 to B2 request",
       ?_assertMatch({ok, 4}, tcall(b1, 5001, 1))},
      {"B2 to B1 request",
       ?_assertMatch({ok, 2}, tcall(b2, 5000, 1))},
      {"B1: loop",
       ?_assertMatch({ok, 2}, tcall(b1, 5000, 1))},
      {"B2: loop",
       ?_assertMatch({ok, 4}, tcall(b2, 5001, 1))},
      {"B1 to B3 long request",
       ?_assertMatch({ok, 3}, tcall(b1, 5002, 3, 2000))},
      {"B1 to B3 request timeout",
       ?_assertMatch({error, timeout}, tcall(b1, 5002, 3))},
      {"B3 to B1 fast request",
       ?_assertMatch({ok, 6}, tcall(b3, 5000, 3))},
      {"B3: loop",
       ?_assertMatch({ok, 999}, tcall(b3, 5002, 999, 2000))},
      {"B3: loop timeout",
       ?_assertMatch({error, timeout}, tcall(b3, 5002, 999))},
      {"B1 to B2: make it crash",
       ?_assertMatch(
          {error, {crash_report, _}},
          tcall(b1, 5001, z))}
     ]}.

tcall(BridgeRef, Port, Request) ->
    tcall(BridgeRef, Port, Request, 1000).

tcall(BridgeRef, Port, Request, Timeout) ->
    case call(BridgeRef, "127.0.0.1", Port,
              term_to_binary(Request), Timeout) of
        {ok, Reply} ->
            {ok, binary_to_term(Reply)};
        {error, _Reason} = Error ->
            Error
    end.

-endif.
