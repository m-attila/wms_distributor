%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%%
%%% @end
%%% Created : 08. Nov 2019 09:46
%%%-------------------------------------------------------------------
-module(wms_distributor_actor).
-author("Attila Makra").

-include_lib("wms_state/include/wms_state.hrl").

%% API
-export([init/0,
         login/3,
         logout/2,
         interaction/5,
         interaction_reply/4,
         keepalive/3]).

-spec init() ->
  map().
init() ->
  {ok, Pid} = wms_distributor_load_balancer:start_link(),
  #{pid => Pid}.

-spec login(map(), node(), [identifier_name()]) ->
  {map(), ok}.
login(State, Node, InteractionIDS) ->
  ok = wms_distributor_load_balancer:login(Node, InteractionIDS),
  {State, ok}.

-spec logout(map(), node()) ->
  {map(), ok}.
logout(State, Node) ->
  ok = wms_distributor_load_balancer:logout(Node),
  {State, ok}.

-spec interaction(map(),
                  identifier_name(),
                  identifier_name(),
                  identifier_name(),
                  [{identifier_name(), literal()}]) ->
                   {map(), ok}.
interaction(State, TaskInstanceID, InteractionID, InteractionRequestID, Parameters) ->
  ok = wms_distributor_load_balancer:interaction(TaskInstanceID,
                                                 InteractionID,
                                                 InteractionRequestID,
                                                 Parameters),
  {State, ok}.

-spec interaction_reply(map(), node(), identifier_name(),
                        {ok, map()} | {error, term()}) ->
                         {map(), ok}.
interaction_reply(State, Node, InteractionRequestID, Result) ->
  ok = wms_distributor_load_balancer:interaction_reply(Node,
                                                       InteractionRequestID,
                                                       Result),
  {State, ok}.

-spec keepalive(map(), node(), identifier_name()) ->
  {map(), ok}.
keepalive(State, Node, InteractionRequestID) ->
  ok = wms_distributor_load_balancer:keepalive(Node, InteractionRequestID),
  {State, ok}.