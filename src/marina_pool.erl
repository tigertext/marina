-module(marina_pool).
-include("marina_internal.hrl").

-compile({no_auto_import, [
    node/1
]}).

-export([
    init/0,
    node/0,
    node/1,
    node_id/1,
    start/2,
    stop/1,
    refresh_nodes/5
]).
-export([
    node_up/2,
    node_down/2
]).

%% public
-spec init() ->
    ok.

init() ->
    ets:new(?MODULE, [named_table, public, set]),
    foil:new(?MODULE),
    foil:load(?MODULE).

-spec node() ->
    {ok, atom()} | {error, marina_pool_not_started}.

node() ->
    node(undefined).

-spec node(binary() | undefined) ->
    {ok, atom()} | {error, marina_pool_not_started}.

node(RoutingKey) ->
    case foil:lookup(?MODULE, strategy) of
        {ok, Strategy} ->
            case node(Strategy, RoutingKey) of
                undefined ->
                    {error, marina_pool_not_started};
                {ok, Node} ->
                    {ok, Node};
                {error, _Reason} ->
                    {error, marina_pool_not_started}
            end;
        {error, _Reason} ->
            {error, marina_pool_not_started}
    end.

-spec node_id(binary()) ->
    atom().

node_id(<<A, B, C, D>>) ->
    RpcAddress = lists:flatten(string:join([integer_to_list(X) ||
        X <- [A, B, C, D]], ".")),
    list_to_atom("marina_" ++ RpcAddress).

-spec start(random | token_aware, [{binary(), binary()}]) ->
    ok.

start(random, Nodes) ->
    start_nodes(Nodes, random, 1);
start(token_aware, Nodes) ->
    marina_ring:build(Nodes),
    start_nodes(Nodes, token_aware, 1).

-spec stop(non_neg_integer()) ->
    ok.

stop(0) ->
    foil:delete(?MODULE, strategy),
    foil:load(?MODULE);
stop(N) ->
    {ok, NodeId} = foil:lookup(?MODULE, {node, N}),
    ok = shackle_pool:stop(NodeId),
    ok = foil:delete(?MODULE, {node, N}),
    stop(N - 1).

-spec node_down(atom(), non_neg_integer()) -> no_return().
node_down(PoolName, FailedWorkerCount) ->
    shackle_utils:warning_msg(?MODULE, "cassandra connection pool down! ~p failed workers ~p", [PoolName, FailedWorkerCount]),
    ets:insert(?MODULE, {PoolName, true}).

-spec node_up(atom(), non_neg_integer()) -> no_return().
node_up(PoolName, FailedWorkerCount) ->
    shackle_utils:warning_msg(?MODULE, "cassandra connection pool up! ~p failed workers ~p", [PoolName, FailedWorkerCount]),
    ets:delete(?MODULE, PoolName).

%% private
node(Strategy, RoutingKey) ->
    node(Strategy, RoutingKey, 1).

node({_, NodeCount}, undefined, N) when N >= NodeCount ->
    %% too many failures, pick first.
    foil:lookup(?MODULE, {node, 1});
node({random, NodeCount} = Strategy, undefined, N) ->
    X = shackle_utils:random(NodeCount),
    check_node(foil:lookup(?MODULE, {node, X}), Strategy, undefined, N);
node({token_aware, NodeCount} = Strategy, undefined, N) ->
    X = shackle_utils:random(NodeCount),
    check_node(foil:lookup(?MODULE, {node, X}), Strategy, undefined, N);
node({token_aware, _NodeCount} = Strategy, RoutingKey, N) ->
    check_node(marina_ring:lookup(RoutingKey), Strategy, RoutingKey, N).

check_node({error, _}, Strategy, _RoutingKey, N) ->
    %% cannot find a proper node when routing,
    %% remove the routing key, so the node selection will fall back to
    %% random or roken_aware without routing key.
    node(Strategy, undefined, N + 1);
check_node({ok, Node}, Strategy, _RoutingKey, N) ->
    case is_node_down(Node) of
        true ->
            shackle_utils:warning_msg(?MODULE, "get a dead node when finding node, node id ~p, retrying", [Node]),
            %% the selected node is marked as down.
            %% remove routing key to fallback to random or token_aware without routing key.
            node(Strategy, undefined, N + 1);
        false ->
            %% shackle_utils:warning_msg(?MODULE, "routing with node ~p", [Node]),
            {ok, Node}
    end.

start_nodes([], random, N) ->
    foil:insert(?MODULE, strategy, {random, N - 1}),
    foil:load(?MODULE);
start_nodes([], token_aware, N) ->
    foil:insert(?MODULE, strategy, {token_aware, N - 1}),
    foil:load(?MODULE);
start_nodes([{RpcAddress, _Tokens} | T], Strategy, N) ->
    case start_node(RpcAddress) of
        {ok, NodeId} ->
            shackle_utils:warning_msg(?MODULE, "node ~p started ", [RpcAddress]),
            foil:insert(?MODULE, {node, N}, NodeId),
            start_nodes(T, Strategy, N + 1);
        {error, pool_already_started} ->
            shackle_utils:warning_msg(?MODULE, "node ~p cannot be started,  pool_already_started", [RpcAddress]),
            start_nodes(T, Strategy, N + 1);
        {error, _Reason} ->
            shackle_utils:warning_msg(?MODULE, "node ~p cannot be started,  reason ~p", [_Reason]),
            start_nodes(T, Strategy, N)
    end.

start_node(<<A, B, C, D>> = RpcAddress) ->
    BacklogSize = ?GET_ENV(backlog_size, ?DEFAULT_BACKLOG_SIZE),
    Ip = lists:flatten(io_lib:format("~b.~b.~b.~b", [A, B, C, D])),
    NodeId = node_id(RpcAddress),
    PoolSize = ?GET_ENV(pool_size, ?DEFAULT_POOL_SIZE),
    PoolStrategy = ?GET_ENV(pool_strategy, ?DEFAULT_POOL_STRATEGY),
    Port = ?GET_ENV(port, ?DEFAULT_PORT),
    Reconnect = ?GET_ENV(reconnect, ?DEFAULT_RECONNECT),
    ReconnectTimeMax = ?GET_ENV(reconnect_time_max,
        ?DEFAULT_RECONNECT_MAX),
    ReconnectTimeMin = ?GET_ENV(reconnect_time_min,
        ?DEFAULT_RECONNECT_MIN),
    SocketOptions = ?GET_ENV(socket_options, ?DEFAULT_SOCKET_OPTIONS),
    PoolFailureThresholdPercentage = ?GET_ENV(pool_failure_threshold_percentage, 0.2),
    PoolRecoverThresholdPercentage = ?GET_ENV(pool_recover_threshold_percentage, 0.1),
    ClientOptions = [
        {ip, Ip},
        {port, Port},
        {reconnect, Reconnect},
        {reconnect_time_max, ReconnectTimeMax},
        {reconnect_time_min, ReconnectTimeMin},
        {socket_options, SocketOptions}
    ],
    PoolOptions = [
        {backlog_size, BacklogSize},
        {pool_size, PoolSize},
        {pool_strategy, PoolStrategy},
        {pool_failure_threshold_percentage, PoolFailureThresholdPercentage},
        {pool_recover_threshold_percentage, PoolRecoverThresholdPercentage},
        {pool_failure_callback_module, ?MODULE},
        {pool_recover_callback_module, ?MODULE}
    ],
    ets:delete(?MODULE, NodeId),
    case shackle_pool:start(NodeId, ?CLIENT, ClientOptions, PoolOptions) of
        ok ->
            {ok, NodeId};
        {error, Reason} ->
            {error, Reason}
    end.

-spec refresh_nodes(list(), list(), random | token_aware, list(), list()) -> ok.
refresh_nodes([], [], _Strategy, _OldNodes, _NewNodes) ->
    ok;
refresh_nodes(NodesToStart, NodesToStop, Strategy, OldNodes, NewNodes) ->
    rebuild_routing(NodesToStart, NodesToStop, Strategy, length(OldNodes), length(NewNodes)),
    (Strategy == token_aware) andalso marina_ring:build(NewNodes),
    foil:insert(?MODULE, strategy, {Strategy, length(NewNodes)}),
    foil:load(?MODULE),
    ok.

%% rebuild the routing table
%% if length(oldnodes) =< length(newnodes) => some node(s) have been added, and/or some node(s) have been switched,
%%      replace old nodes with new nodes for the xisting routing key
%%      find the node index which has been removed, set the index (n) to the new node id;
%% if legnth(oldnodes) > length(new nodes) => some node(s) have been removed, and/or some node(s) hav been switched
%%      Delete the top M-N routing key, move those nodes to nodes to be started (filter out stopped)
%%      Find the spared routing slots in 1-n, set the index (n) to the new node id;
rebuild_routing(NodesToStart, NodesToStop, Strategy, N, M) when N =< M ->
    NodeIndexRemoved = lookup_nodes(NodesToStop, N),
    shackle_utils:warning_msg(?MODULE, "N <= M, Nodes to stop ~p NodeIndexRemoved ~p", [NodesToStop, NodeIndexRemoved]),
    rebuild_routing(NodesToStart, NodeIndexRemoved, Strategy, N + 1),
    stop_nodes(NodesToStop);

rebuild_routing(NodesToStart, NodesToStop, Strategy, N, M) when N > M ->
    NodeIndexRemoved = lookup_nodes(NodesToStop, N),
    shackle_utils:warning_msg(?MODULE, "N > M, Nodes to stop ~p NodeIndexRemoved ~p", [NodesToStop, NodeIndexRemoved]),
    NodesRemoved =
        lists:filtermap(fun(X) ->
            case foil:lookup(?MODULE, {node, X}) of
                {ok, V} ->
                    foil:delete(?MODULE, {node, X}),
                    {true, V};
                _ ->
                    false
            end
                        end, lists:seq(M + 1, N)),
    NodesRemovedButNeedStart = NodesRemoved -- NodesToStop,
    shackle_utils:warning_msg(?MODULE, "N > M, Nodes removed ~p NodesRemovedButNeedStart ~p", [NodesRemoved, NodesRemovedButNeedStart]),
    rebuild_routing(NodesRemovedButNeedStart ++ NodesToStart, NodeIndexRemoved, Strategy, N + 1),
    stop_nodes(NodesToStop).

lookup_nodes(Nodes, MaxIndex) ->
    NodeIds = [node_id(RpcAddress) || {RpcAddress, _} <- Nodes],
    lists:filtermap(fun(N) ->
        case foil:lookup(?MODULE, {node, N}) of
            {ok, V} ->
                case lists:member(V, NodeIds) of
                    true ->
                        {true, N};
                    _ ->
                        false
                end;
            _ ->
                false
        end
                    end, lists:seq(1, MaxIndex)
    ).

rebuild_routing([], _, _, _) ->
    ok;

rebuild_routing([{RpcAddress, _Tokens} | T], [], Strategy, N) ->
    shackle_utils:warning_msg(?MODULE, "rebuilding routing for ~p use index ~p", [RpcAddress, N]),
    start_nodes([{RpcAddress, _Tokens}], Strategy, N),
    rebuild_routing(T, [], Strategy, N + 1);
rebuild_routing([{RpcAddress, _Tokens} | T], [Index | T1], Strategy, N) ->
    shackle_utils:warning_msg(?MODULE, "rebuilding routing for ~p use index ~p", [RpcAddress, Index]),
    start_nodes([{RpcAddress, _Tokens}], Strategy, Index),
    rebuild_routing(T, T1, Strategy, N);
rebuild_routing([NodeId | T], [], Strategy, N) ->
    shackle_utils:warning_msg(?MODULE, "rebuilding routing for ~p use index ~p", [NodeId, N]),
    foil:insert(?MODULE, {node, N}, NodeId),
    rebuild_routing(T, [], Strategy, N + 1);
rebuild_routing([NodeId | T], [Index | T1], Strategy, N) ->
    shackle_utils:warning_msg(?MODULE, "rebuilding routing for ~p use index ~p", [NodeId, Index]),
    foil:insert(?MODULE, {node, Index}, NodeId),
    rebuild_routing(T, T1, Strategy, N).

stop_nodes(NodesToStop) ->
    [
        begin
            NodeId = node_id(RpcAddress),
            ets:insert(?MODULE, {NodeId, true}),
            shackle_pool:stop(NodeId)
        end || {RpcAddress, _} <- NodesToStop
    ].

is_node_down(NodeName) ->
    ets:lookup(?MODULE, NodeName) /= [].

