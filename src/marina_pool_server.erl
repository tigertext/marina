-module(marina_pool_server).
-include("marina_internal.hrl").

-export([
    start_link/0
]).

%% metal callbacks
-export([
    init/3,
    handle_msg/2,
    terminate/2
]).

-define(MSG_BOOTSTRAP, bootstrap_pool).
-define(MSG_PEER_WATCHER, peer_watch).
-define(PEER_WATCH_INTERVAL, 60000).

-record(state, {
    bootstrap_ips :: list(),
    datacenter    :: undefined | binary(),
    node_count    :: undefined | pos_integer(),
    nodes         :: list(),
    port          :: pos_integer(),
    strategy      :: random | token_aware,
    timer_ref     :: undefined | reference()
}).

-type state() :: #state {}.

%% public
-spec start_link() ->
    {ok, pid()}.

start_link() ->
    metal:start_link(?MODULE, ?MODULE, undefined).

%% metal callbacks
-spec init(atom(), pid(), undefined) ->
    no_return().

init(_Name, _Parent, undefined) ->
    BootstrapIps = ?GET_ENV(bootstrap_ips, ?DEFAULT_BOOTSTRAP_IPS),
    Port = ?GET_ENV(port, ?DEFAULT_PORT),
    Strategy = ?GET_ENV(strategy, ?DEFAULT_STRATEGY),

    self() ! ?MSG_BOOTSTRAP,

    {ok, #state {
        bootstrap_ips = BootstrapIps,
        port = Port,
        strategy = Strategy
    }}.

-spec handle_msg(term(), state()) ->
    {ok, state()}.

handle_msg(?MSG_BOOTSTRAP, #state {
        bootstrap_ips = BootstrapIps,
        port = Port,
        strategy = Strategy
    } = State) ->

    case nodes(BootstrapIps, Port) of
        {ok, Nodes} ->
            marina_pool:start(Strategy, Nodes),
            timer:send_after(1000, self(), ?MSG_PEER_WATCHER),
            {ok, State#state {
                node_count = length(Nodes),
                nodes = Nodes
            }};
        {error, _Reason} ->
            shackle_utils:warning_msg(?MODULE, "bootstrap failed~n", []),
            {ok, State#state {
                timer_ref = erlang:send_after(500, self(), ?MSG_BOOTSTRAP)
            }}
    end;
handle_msg(?MSG_PEER_WATCHER, #state {
        bootstrap_ips = BootstrapIps,
        port = Port,
        strategy = Strategy,
        nodes = OldNodes
    } = State) ->
    case nodes(BootstrapIps, Port) of
        {ok, Nodes} ->
            NodesToStart = Nodes -- OldNodes,
            NodesToStop = OldNodes -- Nodes,
            stop_nodes(NodesToStop, Strategy, Nodes),
            start_nodes(Strategy, NodesToStart, Nodes),
            timer:send_after(?PEER_WATCH_INTERVAL, self(), ?MSG_PEER_WATCHER),
            {ok, State#state{
                node_count = length(Nodes),
                nodes = Nodes
            }};

        {error, Reason} ->
            shackle_utils:warning_msg(?MODULE, "failed to refresh cassandra peers ~p~n", [Reason]),
            {ok, State#state{
                timer_ref = erlang:send_after(500, self(), ?MSG_PEER_WATCHER)
            }}
    end.


-spec terminate(term(), state()) ->
    ok.

terminate(_Reason, #state {node_count = NodeCount}) ->
    marina_pool:stop(NodeCount),
    ok.

%% private
connect(Ip, Port) ->
    case marina_utils:connect(Ip, Port) of
        {ok, Socket} ->
            case marina_utils:startup(Socket) of
                {ok, undefined} ->
                    {ok, Socket};
                {ok, <<"org.apache.cassandra.auth.PasswordAuthenticator">>} ->
                    case marina_utils:authenticate(Socket) of
                        ok ->
                            {ok, Socket};
                        {error, Reason} ->
                            gen_tcp:close(Socket),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    gen_tcp:close(Socket),
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

filter_datacenter([], _Datacenter) ->
    [];
filter_datacenter([[RpcAddress, _Datacenter, Tokens] | T], undefined) ->
    [{RpcAddress, Tokens} | filter_datacenter(T, undefined)];
filter_datacenter([[RpcAddress, Datacenter, Tokens] | T], Datacenter) ->
    [{RpcAddress, Tokens} | filter_datacenter(T, Datacenter)];
filter_datacenter([_ | T], Datacenter) ->
    filter_datacenter(T, Datacenter).

nodes([], _Port) ->
    {error, bootstrap_failed};
nodes([Ip | T], Port) ->
    case peers(Ip, Port) of
        {ok, Rows, Datacenter} ->
            case filter_datacenter(Rows, Datacenter) of
                [] ->
                    nodes(T, Port);
                Nodes ->
                    {ok, Nodes}
            end;
        {error, Reason} ->
            shackle_utils:warning_msg(?MODULE,
                "bootstrap error: ~p~n", [Reason]),
            nodes(T, Port)
    end.

peers(Ip, Port) ->
    case connect(Ip, Port) of
        {ok, Socket} ->
            V = peers_query(Socket),
            gen_tcp:close(Socket),
            V;
        {error, Reason} ->
            {error, Reason}
    end.

peers_query(Socket) ->
    {ok, {result, _ , _, Rows}} = marina_utils:query(Socket, ?LOCAL_QUERY),
    [[_RpcAddress, Datacenter, _Tokens]] = Rows,
    {ok, {result, _ , _, Rows2}} = marina_utils:query(Socket, ?PEERS_QUERY),
    {ok, Rows ++ Rows2, Datacenter}.

%% start new nodes found in cassandra cluster.
start_nodes(_Strategy, [], _OldNodes) ->
    ok;
%% the reason why to start all nodes instead of nodestostart is
%% because marina_ring needs all nodes to build the ring.
%% for started nodes, it will not be started again.
start_nodes(Strategy, NodesToStart, NewNodes) ->
    shackle_utils:warning_msg(?MODULE, "found new nodes, starting ~p", [NodesToStart]),
    %% make sure the new nodes are in the end of the list
    %% so foil will not overwrite random settings
    marina_pool:start_nodes(_rebuild_foil = false, NodesToStart, Strategy, length(NewNodes)).

%% stop nodes removed from cassandra cluster.
stop_nodes([], _Strategy, _Nodes) ->
    ok;
stop_nodes(NodesToStop, Strategy, Nodes) ->
    shackle_utils:warning_msg(?MODULE, "nodes get removed from cluster, stopping worker pool ~p", [NodesToStop]),
    marina_pool:stop_nodes(NodesToStop, Strategy, Nodes).