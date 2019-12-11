%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%%
%%% @end
%%% Created : 08. Nov 2019 09:47
%%%-------------------------------------------------------------------
-module(wms_distributor_load_balancer).
-author("Attila Makra").

%% API
-behaviour(gen_server).

-include_lib("wms_common/include/wms_common.hrl").
-include_lib("wms_logger/include/wms_logger.hrl").
-include_lib("wms_state/include/wms_state.hrl").

%% API
-export([start_link/0,
         login/2,
         logout/1,
         interaction/4,
         interaction_reply/3,
         keepalive/2]).

-export([init/1,
         handle_info/2,
         handle_call/3,
         handle_cast/2]).

%% =============================================================================
%% Private types
%% =============================================================================
-type phase() :: subscribe | wait_for_cluster_ready | explore_operator_actors | started.


-record(state, {
  phase = subscribe :: phase(),
  interactors :: wms_distributor_iservers:interaction_servers()
}).
-type state() :: #state{}.

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() ->
  {ok, pid()} | ignore | {error, {already_started, pid()} | term()}.
start_link() ->
  Ret = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
  ?info("stared."),
  Ret.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% login/2
%% ###### Purpose
%% Login operator actor on node, for given interactions.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec login(node(), [identifier_name()]) ->
  ok | {error, term()}.
login(Node, InteractionIDS) ->
  gen_server:call(?MODULE, {login, Node, InteractionIDS}).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% logout/1
%% ###### Purpose
%% Logout operator on give node
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec logout(node()) ->
  ok | {error, term()}.
logout(Node) ->
  gen_server:call(?MODULE, {logout, Node}).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% interaction/4
%% ###### Purpose
%% Start interaction.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec interaction(identifier_name(),
                  identifier_name(),
                  identifier_name(),
                  [{identifier_name(), term()}]) ->
                   any().
interaction(TaskInstanceID, InteractionID, InteractionRequestID, Parameters) ->
  gen_server:cast(?MODULE, {interaction, TaskInstanceID,
                            InteractionID, InteractionRequestID, Parameters}).

-spec interaction_reply(node(), identifier_name(), {ok, map} | {error, term()}) ->
  ok.
interaction_reply(Node, InteractionRequestID, Result) ->
  gen_server:cast(?MODULE, {interaction_reply, Node, InteractionRequestID, Result}).

-spec keepalive(node(), identifier_name()) ->
  ok.
keepalive(Node, InteractionRequestID) ->
  gen_server:cast(?MODULE, {keepalive, Node, InteractionRequestID}).

%% =============================================================================
%% gen_server behaviour
%% =============================================================================

-spec init(Args :: term()) ->
  {ok, State :: state()}.
init(_) ->
  process_flag(trap_exit, true),
  self() ! start,
  {ok, #state{interactors = wms_distributor_iservers:new()}}.

-spec handle_info(Info :: any(), State :: state()) ->
  {noreply, State :: state()}.

%% -----------------------------------------------------------------------------
%% Process next phase
%% -----------------------------------------------------------------------------

handle_info(start, State) ->
  {noreply, handle_phase(State)};
handle_info({nodeup, Node}, #state{interactors = Interactors} = State) ->
  ?info("~s node is coming up", [Node]),

  {BrokenInteractions, NewInteractors} = wms_distributor_iservers:up(Node,
                                                                     Interactors),
  broken_interactions(BrokenInteractions),
  {noreply, State#state{interactors = NewInteractors}};
handle_info({nodedown, Node}, #state{interactors = Interactors} = State) ->
  ?info("~s node is going down", [Node]),

  {BrokenInteractions, NewInteractors} = wms_distributor_iservers:down(Node,
                                                                       Interactors),
  broken_interactions(BrokenInteractions),
  {noreply, State#state{interactors = NewInteractors}};

handle_info(Msg, State) ->
  ?warning("Unknown message: ~0p", [Msg]),
  {noreply, State}.


-spec handle_call(Info :: any(), From :: {pid(), term()}, State :: state()) ->
  {reply, term(), State :: state()}.
handle_call(Cmd, _From, #state{phase = Phase} = State) when Phase =/= started ->
  ?error("DRT-0001",
         "~p command was received, but not initialized yet", [Cmd]),

  {reply, {error, not_initialized}, State};
handle_call({login, Node, InteractionIDS}, _From, #state{interactors = Interactors} = State) ->
  ?info("~s node was logged in for ~0p interaction", [Node, InteractionIDS]),
  {reply, ok, State#state{interactors =
                          wms_distributor_iservers:login(Node,
                                                         InteractionIDS,
                                                         Interactors)}};
handle_call({logout, Node}, _From, #state{interactors = Interactors} = State) ->
  ?info("~s node was logged out", [Node]),
  {reply, ok, State#state{interactors =
                          wms_distributor_iservers:logout(Node, Interactors)}};

handle_call(_, _From, State) ->
  {reply, ok, State}.

-spec handle_cast(Request :: any(), State :: state()) ->
  {noreply, State :: state()}.
handle_cast({interaction, TaskInstanceID, InteractionID, InteractionRequestID, Parameters}, State) ->
  {noreply, interaction_request(TaskInstanceID, InteractionID,
                                InteractionRequestID, Parameters, State)};
handle_cast({interaction_reply, Node, InteractionRequestID, Result}, State) ->
  {noreply, interaction_reply(Node, InteractionRequestID, Result, State)};
handle_cast({keepalive, Node, InteractionRequestID}, State) ->
  {noreply, keepalive(Node, InteractionRequestID, State)};

handle_cast(_, State) ->
  {noreply, State}.

%% =============================================================================
%% Private functions
%% =============================================================================

%% -----------------------------------------------------------------------------
%% Phase handling
%% -----------------------------------------------------------------------------

handle_phase(#state{phase = subscribe} = State) ->
  ok = wms_dist:subscribe_node_status(self()),
  ?debug("Subscribed for node status changes"),
  self() ! start,
  State#state{phase = wait_for_cluster_ready};
handle_phase(#state{phase = wait_for_cluster_ready} = State) ->
  case wms_dist:is_cluster_connected() of
    false ->
      erlang:send_after(500, self(), start),
      State;
    true ->
      ?info("Cluster is connected, starting to expore operator actors"),
      self() ! start,
      State#state{phase = explore_operator_actors}
  end;
handle_phase(#state{phase = explore_operator_actors} = State) ->
  rpc:multicall(
    wms_dist:get_dst_nodes(connected) ++ wms_dist:get_dst_opc_nodes(connected),
    wms_operator_actor,
    login,
    []),
  State#state{phase = started}.

%% -----------------------------------------------------------------------------
%% Interaction handling
%% -----------------------------------------------------------------------------

-spec interaction_request(identifier_name(),
                          identifier_name(),
                          identifier_name(),
                          [{identifier_name(), literal()}],
                          state()) ->
                           state().
interaction_request(TaskInstanceID,
                    InteractionID,
                    InteractionRequestID,
                    Parameters,
                    #state{interactors = Interactors} = State) ->
  case wms_distributor_iservers:get_actor_node(InteractionID, Interactors) of
    undefined ->
      % no operator actor
      ?error("DRT-0002",
             "Operator actor was not found for ~s task, to execute ~s "
             "interaction which is identified by ~s",
             [TaskInstanceID, InteractionID, InteractionRequestID]),
      ok = wms_dist:call(wms_engine_actor,
                         [TaskInstanceID,
                          InteractionID,
                          InteractionRequestID,
                          {error, not_found}]),
      State;
    Node ->
      call_interaction(TaskInstanceID,
                       InteractionID,
                       InteractionRequestID,
                       Parameters, Node, State)
  end.

-spec call_interaction(identifier_name(),
                       identifier_name(),
                       identifier_name(),
                       [{identifier_name(), literal()}],
                       node(),
                       state()) ->
                        state().
call_interaction(TaskInstanceID,
                 InteractionID,
                 InteractionRequestID,
                 Parameters,
                 Node,
                 #state{interactors = Interactors} = State) ->

  case
    rpc:call(Node, wms_operator_actor, interaction, [InteractionRequestID,
                                                     InteractionID,
                                                     Parameters]) of
    {badrpc, Reason} ->
      ?error("DRT-0003",
             "Unable to call ~s interaction from ~s "
             " which is identified by ~s on node ~s. Reason : ~0p",
             [InteractionID, TaskInstanceID, InteractionRequestID, Node, Reason]),
      ok = wms_dist:call(wms_engine_actor,
                         interaction_reply,
                         [TaskInstanceID,
                          InteractionID,
                          InteractionRequestID,
                          {error, Reason}]),
      State;

    ok ->
      ?debug("~s interacion request was sent to node ~s"
             " from task ~s. Request identified by ~s",
             [InteractionID, Node, InteractionRequestID, TaskInstanceID]),
      State#state{interactors =
                  wms_distributor_iservers:start_interaction(
                    Node,
                    InteractionRequestID,
                    TaskInstanceID,
                    InteractionID,
                    Interactors)}

  end.


-spec interaction_reply(node(), identifier_name(), {ok, map()} | {error, term()}, state()) ->
  state().
interaction_reply(Node, InteractionRequestID, Result,
                  #state{interactors = Interactors} = State) ->
  NewInteractors =
    case wms_distributor_iservers:stop_interaction(Node, InteractionRequestID, Interactors) of
      {undefined, Interactors2} ->
        ?error("DRT-0004",
          "No task was found for interaction reply from ~s node, ~s with "
          "interaction request ID", [Node, InteractionRequestID]),
        Interactors2;
      {{TaskInstanceID, InteractionID}, Interactors2} ->
        ok = wms_dist:call(wms_engine_actor,
                           interaction_reply,
                           [TaskInstanceID, InteractionID,
                            InteractionRequestID, Result]),
        Interactors2
    end,
  State#state{interactors = NewInteractors}.

broken_interactions([]) ->
  ok;
broken_interactions([#{task_instance_id := TaskInstanceID,
                       interaction_id := InteractionID,
                       interaction_request_id := InteractionRequestID} | Rest]) ->
  ?error("DRT-0005", "~s interaction was broken", [InteractionID]),
  ok = wms_dist:call(wms_engine_actor,
                     interaction_reply,
                     [TaskInstanceID, InteractionID,
                      InteractionRequestID, {error, broken}]),
  broken_interactions((Rest)).


-spec keepalive(node(), identifier_name(), state()) ->
  state().
keepalive(Node, InteractionRequestID, #state{interactors = Interactors} = State) ->
  case wms_distributor_iservers:get_interaction_data(Node, InteractionRequestID, Interactors) of
    undefined ->
      ?error("DRT-0006",
             "No task was found on ~s node, ~s interaction request ID",
             [Node, InteractionRequestID]),
      ok;
    {TaskInstanceID, InteractionID} ->
      ok = wms_dist:call(wms_engine_actor,
                         keepalive,
                         [TaskInstanceID, InteractionID, InteractionRequestID])

  end,
  State.
