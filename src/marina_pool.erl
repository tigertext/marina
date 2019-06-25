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
    stop/1
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
    start(Nodes, random, 1);
start(token_aware, Nodes) ->
    marina_ring:build(Nodes),
    start(Nodes, token_aware, 1).

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

node_down(PoolName, _FailedWorkerCount) ->
    shackle_utils:warning_msg(?MODULE, "node down!!! node down!!!", [PoolName]),
    ets:insert(?MODULE, {PoolName, true}).
node_up(PoolName, _FailedWorkerCount) ->
    shackle_utils:warning_msg(?MODULE, "node up!!! node up!!!", [PoolName]),
    ets:delete_object(?MODULE, PoolName).

%% private
node({random, NodeCount} = Strategy, undefined) ->
    N = shackle_utils:random(NodeCount),
    check_node(foil:lookup(?MODULE, {node, N}), Strategy, undefined);
node({token_aware, NodeCount} = Strategy, undefined) ->
    N = shackle_utils:random(NodeCount),
    check_node(foil:lookup(?MODULE, {node, N}), Strategy, undefined);
node({token_aware, _NodeCount} = Strategy, RoutingKey) ->
    check_node(marina_ring:lookup(RoutingKey), Strategy, RoutingKey).

check_node({error, _}, Strategy, RoutingKey) ->
    node(Strategy, RoutingKey);
check_node(Node, Strategy, RoutingKey) ->
    case is_node_down(Node) of
        true ->
            shackle_utils:warning_msg(?MODULE, "get a dead node when finding node, node id ~p, retrying", [Node]),
            node(Strategy, RoutingKey);
        false ->
            Node
    end.

start(<<A, B, C, D>> = RpcAddress) ->
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
    PoolFailureThresholdPercentage = ?GET_ENV(pool_failure_threshold_percentage, 0),
    PoolRecoverThresholdPercentage = ?GET_ENV(pool_recover_threshold_percentage, 0),
    FailureCbFun = undefined,
    RecoverCbFun = undefined,
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
        {pool_failure_callback_fn, FailureCbFun},
        {pool_recover_callback_fn, RecoverCbFun}
    ],

    case shackle_pool:start(NodeId, ?CLIENT, ClientOptions, PoolOptions) of
        ok ->
            {ok, NodeId};
        {error, Reason} ->
            {error, Reason}
    end.

start([], random, N) ->
    foil:insert(?MODULE, strategy, {random, N - 1}),
    foil:load(?MODULE);
start([], token_aware, N) ->
    foil:insert(?MODULE, strategy, {token_aware, N - 1}),
    foil:load(?MODULE);
start([{RpcAddress, _Tokens} | T], Strategy, N) ->
    case start(RpcAddress) of
        {ok, NodeId} ->
            foil:insert(?MODULE, {node, N}, NodeId),
            start(T, Strategy, N + 1);
        {error, _Reason} ->
            start(T, Strategy, N)
    end.

is_node_down(NodeName) ->
    ets:lookup(?MODULE, NodeName) /= [].