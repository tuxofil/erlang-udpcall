%%%-------------------------------------------------------------------
%%% File        : udpcall.hrl
%%% Author      : Aleksey Morarash <aleksey.morarash@gmail.com>
%%% Description : udpcall definitions file
%%% Created     : 31 Oct 2014
%%%-------------------------------------------------------------------

-ifndef(_UDPCALL).
-define(_UDPCALL, true).

%% UDP message type markers
-define(REQ, 0).
-define(REP, 1).
-define(REP_CRASH, 2).

%% UDP packets format
-define(
   REQ_PACKET(Cookie, ExpireTime, Request),
   <<?REQ:8/big-unsigned,
     Cookie:32/big-unsigned,
     ExpireTime:64/big-unsigned,
     Request/binary>>).

-define(
   REP_PACKET(Cookie, Reply),
   <<?REP:8/big-unsigned,
     Cookie:32/big-unsigned,
     Reply/binary>>).

-define(
   REP_CRASH_PACKET(Cookie, Reason),
   <<?REP_CRASH:8/big-unsigned,
     Cookie:32/big-unsigned,
     Reason/binary>>).

%% Control messages (local process requests to the remote side):
-define(
   LCALL(From, Ref, ToHost, ToPort, Request, Timeout),
   {lcall, From, Ref, ToHost, ToPort, Request, Timeout}).
-define(LRESP(Ref, Result), {lresp, Ref, Result}).

%% Control messages (remote side requests to the receiver process):
-define(
   RCALL(BridgeRef, From, Request),
   {rcall, BridgeRef, From, Request}).
-define(RRESP(To, Reply), {rresp, To, Reply}).
-define(RRESP_CRASH(To, Reason), {rresp_crash, To, Reason}).

%% Conntrack record (from local processes to a remote side):
-record(
   lconntrack,
   {cookie :: cookie(),
    ref :: reference(),
    sender :: pid(),
    expires :: expire_time()
   }).

%% Conntrack record (from a remote side to a local process):
-record(
   rconntrack,
   {ref :: reference(),
    cookie :: cookie(),
    peername :: peername(),
    expires :: expire_time()
   }).

%% ----------------------------------------------------------------------
%% eunit

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-endif.
