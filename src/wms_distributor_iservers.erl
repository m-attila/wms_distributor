%%%-------------------------------------------------------------------
%%% @author Attila Makra
%%% @copyright (C) 2019, Attila Makra.
%%% @doc
%%% @end
%%% Created : 08. Nov 2019 13:23
%%%-------------------------------------------------------------------
-module(wms_distributor_iservers).
-author("Attila Makra").

-include_lib("wms_common/include/wms_common.hrl").
-include_lib("wms_state/include/wms_state.hrl").

%% API
-export([new/0,
         down/2,
         up/2,
         start_interaction/5,
         stop_interaction/3,
         login/3,
         logout/2,
         get_actor_node/2,
         get_interaction_data/3]).

%% =============================================================================
%% Types
%% =============================================================================

-export_type([interaction_data/0,
              interaction_servers/0,
              broken_interaction/0]).

-type node_status() :: up | down.

-type interaction_data() :: {Started :: timestamp(),
                             TaskInstanceID :: identifier_name(),
                             Interaction :: identifier_name()}.


%% @formatter:off

-type broken_interaction() :: #{
  task_instance_id := binary(),
  interaction_id := binary(),
  interaction_request_id := binary()
}.

-type running_interactions() :: #{
  InteractionRequestID :: binary() => interaction_data()
}.

-type node_interactions() :: #{Interaction :: identifier_name() => [node()]}.

-type node_data() :: #{
  status := node_status(),
  last_run := timestamp() | undefined,
  running := running_interactions()
}.

-type interaction_servers() :: #{
  interactions := node_interactions(),
  nodes := #{node() => node_data()}}.
%% @formatter:on

%% =============================================================================
%% API
%% =============================================================================

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% new/0
%% ###### Purpose
%% Create new interaction_server instance.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec new() ->
  interaction_servers().
new() ->
  #{
    interactions => #{},
    nodes => #{}
  }.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% down/2
%% ###### Purpose
%% Handle nodedown signal
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec down(node(), interaction_servers()) ->
  {[broken_interaction()], interaction_servers()}.
down(Node, #{nodes := Nodes} = Servers) ->
  change_node_status(Node, Servers, down, maps:get(Node, Nodes, undefined)).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% up/2
%% ###### Purpose
%% Handle nodeup signal.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec up(node(), interaction_servers()) ->
  {[broken_interaction()], interaction_servers()}.
up(Node, #{nodes := Nodes} = Servers) ->
  change_node_status(Node, Servers, up, maps:get(Node, Nodes, undefined)).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% start_interaction/4
%% ###### Purpose
%% Register started interaction on node.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec start_interaction(node(), identifier_name(), identifier_name(),
                        identifier_name(), interaction_servers()) ->
                         interaction_servers().
start_interaction(Node, InteractionRequestID, TaskInstanceID, InteractionID,
                  #{nodes := Nodes} = Servers) ->
  add_interaction(Node, InteractionRequestID, TaskInstanceID, InteractionID,
                  Servers, maps:get(Node, Nodes, undefined)).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% stop_interaction/3
%% ###### Purpose
%% Handle stop_interaction event.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec stop_interaction(node(), identifier_name(), interaction_servers()) ->
  {{TaskInstanceID :: identifier_name(), InteractionID :: identifier_name()} |
   undefined,
   interaction_servers()}.
stop_interaction(Node, InteractionRequestID, #{nodes := Nodes} = Servers) ->
  rem_interaction(Node, InteractionRequestID, Servers, maps:get(Node, Nodes, undefined)).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% get_interaction_data/2
%% ###### Purpose
%% Returns task instance id and interaction id from interaction request id.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec get_interaction_data(node(), identifier_name(), interaction_servers()) ->
  {TaskInstanceID :: identifier_name(), InteractionID :: identifier_name()} |
  undefined.
get_interaction_data(Node, InteractionRequestID, #{nodes := Nodes}) ->
  get_interaction_data(InteractionRequestID, maps:get(Node, Nodes, undefined)).

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% login/3
%% ###### Purpose
%% Operator actor was logged in for given interactions
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec login(node(), [identifier_name()], interaction_servers()) ->
  interaction_servers().
login(Node, NodeInteractions, #{interactions := Interactions,
                                nodes := NodesData} = Servers) ->
  NewInteractions = add_interaction(Node, NodeInteractions, Interactions),
  Servers#{interactions := NewInteractions,
           nodes := NodesData#{
             Node =>#{
               status => up,
               running => #{},
               last_run => undefined
             }
           }}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% logout/2
%% ###### Purpose
%% Operaton actor logged out.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end

-spec logout(node(), interaction_servers()) ->
  interaction_servers().
logout(Node, #{interactions := Interactions} = Servers) ->
  NewInteractions = rem_node_interactions(Node, Interactions),
  Servers#{interactions := NewInteractions}.

%% @doc
%%
%%-------------------------------------------------------------------
%%
%% ### Function
%% get_actor_node/2
%% ###### Purpose
%% Return node which can serve interaction and the task number is less than
%% other ones.
%% ###### Arguments
%%
%% ###### Returns
%%
%%-------------------------------------------------------------------
%%
%% @end
-spec get_actor_node(identifier_name(), interaction_servers()) ->
  node() | undefined.
get_actor_node(InteractionID, #{interactions := Interactions,
                                nodes := NodesData}) ->
  % nodes, to serve interaction
  NodesForInteractions = maps:get(InteractionID, Interactions, []),

  AvailableNodes =
    lists:filtermap(
      fun(Node) ->
        case maps:get(Node, NodesData) of
          #{status := up,
            running :=Running} ->
            {true, {Node, map_size(Running)}};
          _ ->
            false
        end
      end, NodesForInteractions),


  SortedAvailableNodes =
    lists:sort(
      fun({_, Count1}, {_, Count2}) ->
        Count1 =< Count2
      end,
      AvailableNodes),

  case SortedAvailableNodes of
    [] ->
      undefined;
    [F | _] ->
      {N, _} = F,
      N
  end.


%% =============================================================================
%% Private functions
%% =============================================================================

-spec change_node_status(node(),
                         interaction_servers(),
                         node_status(),
                         node_data() | undefined) ->
                          {[broken_interaction()], interaction_servers()}.
change_node_status(_, Servers, _, undefined) ->
  % node not found
  {[], Servers};
change_node_status(Node, Servers, NodeStatus, #{running := Running} = NodeData) ->
  BrokenTaskInstances = maps:fold(
    fun(InteractionRequestID, {_, TaskInstanceID, InteractionID}, Accu) ->
      [#{task_instance_id => TaskInstanceID,
         interaction_id => InteractionID,
         interaction_request_id => InteractionRequestID} | Accu]
    end, [], Running),


  NewNodeData = NodeData#{status := NodeStatus,
                          running := #{}},
  {BrokenTaskInstances,
   Servers#{nodes := #{Node => NewNodeData}}}.




-spec add_interaction(node(),
                      identifier_name(),
                      identifier_name(),
                      identifier_name(),
                      interaction_servers(),
                      node_data() | undefined) ->
                       interaction_servers().
add_interaction(_, _, _, _, Servers, undefined) ->
  % node not found
  Servers;
add_interaction(Node, InteractionRequestID, TaskInstanceID, InteractionID,
                #{nodes := Nodes} = Servers,
                #{running := Running} = NodeData) ->
  NewNodeData = NodeData#{running :=
                          Running#{InteractionRequestID =>
                                   {wms_common:timestamp(),
                                    TaskInstanceID, InteractionID}
                          },
                          last_run := wms_common:timestamp()},
  Servers#{nodes := Nodes#{Node => NewNodeData}}.


-spec rem_interaction(node(),
                      identifier_name(),
                      interaction_servers(),
                      node_data() | undefined) ->
                       {{TaskInstanceID :: identifier_name(), InteractionID :: identifier_name()} |
                        undefined,
                        interaction_servers()}.
rem_interaction(_Node, _InteractionRequestID, Servers, undefined) ->
  % node not found
  {undefined, Servers};
rem_interaction(Node, InteractionRequestID, #{nodes := Nodes} = Servers, #{running := Running} = NodeData) ->
  case maps:get(InteractionRequestID, Running, undefined) of
    undefined ->
      {undefined, Servers};
    {_, TaskInstanceID, InteractionID} ->
      NewNodeData = NodeData#{running := maps:remove(InteractionRequestID, Running)},
      NewServers = Servers#{nodes := Nodes#{Node => NewNodeData}},
      {{TaskInstanceID, InteractionID}, NewServers}
  end.

-spec add_interaction(node(), [identifier_name()], node_interactions()) ->
  node_interactions().
add_interaction(_, [], InteractionData) ->
  InteractionData;
add_interaction(Node, [InteractionID | RestInteractionID], InteractionData) ->
  NodesForInteractionID = maps:get(InteractionID, InteractionData, []),

  NewNodesForInteractionID =
    case lists:member(Node, NodesForInteractionID) of
      false ->
        [Node | NodesForInteractionID];
      true ->
        NodesForInteractionID
    end,

  NewInteractionData = InteractionData#{InteractionID =>
                                        NewNodesForInteractionID},
  add_interaction(Node, RestInteractionID, NewInteractionData).

-spec rem_node_interactions(node(), node_interactions()) ->
  node_interactions().
rem_node_interactions(Node, Interactions) ->
  maps:fold(
    fun(IntID, Nodes, Accu) ->
      case lists:delete(Node, Nodes) of
        [] ->
          % no nodes for given interaction yet
          Accu;
        NewNodes ->
          Accu#{IntID => NewNodes}
      end
    end, #{}, Interactions).

-spec get_interaction_data(identifier_name(), node_data() | undefined) ->
  undefined | {identifier_name(), identifier_name()}.
get_interaction_data(_InteractionRequestID, undefined) ->
  undefined;
get_interaction_data(InteractionRequestID, #{running := Running}) ->
  case maps:get(InteractionRequestID, Running, undefined) of
    undefined ->
      undefined;
    {_, TaskInstanceID, InteractionID} ->
      {TaskInstanceID, InteractionID}
  end.
