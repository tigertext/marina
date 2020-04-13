-module(marina_bucket).
-author("anders").
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/1, return_ticket/2, acquire_ticket/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(MAX_TICKET, 1000).
-define(STOP_POLLING_THRESHOLD, 300).
-define(ETS_TABLE, marina_work_pool_tickets).

-record(ticket_bucket, {
    name,
    checkedout_ticket = 0,
    max_ticket = ?MAX_TICKET,
    bucket = bucket:new()
}).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link(any()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(NodeId) ->
    Name = to_name(NodeId),
    gen_server:start_link({local, Name}, ?MODULE, [NodeId], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
    {ok, State :: #{}} | {ok, State :: #{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Name]) ->
    case ets:info(?ETS_TABLE) of
        undefined ->
            ets:new(?ETS_TABLE, [public, set, named_table, {keypos, #ticket_bucket.name}]);
        _ ->
            ok
    end,
    ets:insert(?ETS_TABLE, #ticket_bucket{bucket = dict:new(), name = Name}),
    {ok, #{name => Name}}.

-spec return_ticket(atom(), any()) -> ok.
return_ticket(Pool, Reason) ->
    return_ticket(Pool, Reason, self()).
return_ticket(Pool, Reason, From) ->
    F = fun() ->
        [Ticket = #ticket_bucket{name = Pool, checkedout_ticket = Tickets, bucket = Dict}] = ets:lookup(?ETS_TABLE, Pool),
        lager:debug("release ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
        {Dict1, TicketsToReturn} =
            case dict:find(From, Dict) of
                error ->
                    {Dict, 0};
                {ok, 1} ->
                    {dict:erase(From, Dict), 1};
                {ok, N} when N > 1 ->
                    {dict:store(From, N - 1, Dict), 1}
            end,
        NewMax = adjust_max_tickets(Reason, Ticket),
        ets:update_counter(?ETS_TABLE, Pool, {#ticket_bucket.checkedout_ticket, -1 * TicketsToReturn}),
        ets:update_element(?ETS_TABLE, Pool, {#ticket_bucket.max_ticket, NewMax}),
        ets:update_element(?ETS_TABLE, Pool, {#ticket_bucket.bucket, Dict1})
        end,
    spawn(F).

-spec acquire_ticket(atom()) -> ok | error.
acquire_ticket(Pool) ->
    From = self(),
    [TicketState = #ticket_bucket{name = Pool, checkedout_ticket = Tickets, max_ticket = Max}] = ets:lookup(?ETS_TABLE, Pool),
    case Tickets >= Max of
        true ->
            lager:warning("cannot aacquire tickets!! pool ~p from ~p, waiting for all tickets to be returned, checkedout ~p, max ~p", [Pool, From, Tickets, Max]),
            error;
        false ->
            F = fun() ->
                NewBucket = checkout_ticket(From, TicketState),
                ets:update_counter(?ETS_TABLE, Pool, {#ticket_bucket.checkedout_ticket, 1}),
                ets:update_element(?ETS_TABLE, Pool, {#ticket_bucket.bucket, NewBucket}),
                gen_server:cast(to_name(Pool), {monitor, From})
                end,
            spawn(F)
    end.

stop(Pool) ->
    Name = to_name(Pool),
    ets:delete(?ETS_TABLE, Pool),
    gen_server:call(Name, stop).

%% @private
%% @doc Handling call messages
-spec(handle_call(Request :: term(), From :: {pid(), Tag :: term()},
    State :: #{}) ->
    {reply, Reply :: term(), NewState :: #{}} |
    {reply, Reply :: term(), NewState :: #{}, timeout() | hibernate} |
    {noreply, NewState :: #{}} |
    {noreply, NewState :: #{}, timeout() | hibernate} |
    {stop, Reason :: term(), Reply :: term(), NewState :: #{}} |
    {stop, Reason :: term(), NewState :: #{}}).

handle_call(increase_max_ticket, _, State = #{name := Pool}) ->
    [#ticket_bucket{max_ticket = Max}] = ets:lookup(?ETS_TABLE, Pool),
    case Max < ?STOP_POLLING_THRESHOLD of
        true ->
            schedule_poll(self()),
            ets:update_element(?ETS_TABLE, Pool, {#ticket_bucket.max_ticket, Max + 1}),
            {reply, ok, State};
        false ->
            {reply, ignored, State}
    end;
handle_call(stop, _From, State) ->
    {stop, normal, State}.

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #{}) ->
    {noreply, NewState :: #{}} |
    {noreply, NewState :: #{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #{}}).
handle_cast({monitor, Pid}, State) ->
    erlang:monitor(process, Pid),
    {noreply, State};
handle_cast(_Request, State = #{}) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #{}) ->
    {noreply, NewState :: #{}} |
    {noreply, NewState :: #{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #{}}).
handle_info({'DOWN', _MonitorRef, process, From, _Info}, State = #{name := Pool}) ->
    %% process down
    [#ticket_bucket{bucket = Dict, checkedout_ticket = Tickets}] = ets:lookup(?ETS_TABLE, Pool),
    lager:info("marina release ticket from pool ~p, process down ~p current tickets: ~p", [Pool, From, Tickets]),
    {Dict1, TicketsToReturn} =
        case dict:find(From, Dict) of
            error ->
                %% all tickets for the process have been returned.
                {Dict, 0};
            {ok, N} ->
                {dict:erase(From, Dict), N}
        end,
    ets:update_counter(?ETS_TABLE, Pool, {#ticket_bucket.checkedout_ticket, -1 * TicketsToReturn}),
    ets:update_element(?ETS_TABLE, Pool, {#ticket_bucket.bucket, Dict1}),
    {noreply, State};

handle_info(poll, State = #{name := Pool}) ->
    spawn(fun() -> poll(Pool) end),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%% @private
%% @doc This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
-spec(terminate(Reason :: (normal | shutdown | {shutdown, term()} | term()),
    State :: #{}) -> term()).
terminate(_Reason, _State = #{}) ->
    ok.

%% @private
%% @doc Convert process state when code is changed
-spec(code_change(OldVsn :: term() | {down, term()}, State :: #{},
    Extra :: term()) ->
    {ok, NewState :: #{}} | {error, Reason :: term()}).
code_change(_OldVsn, State = #{}, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

to_name(Pool) ->
    try
        list_to_existing_atom(atom_to_list(Pool) ++ "__bucket")
    catch _:_ ->
        list_to_atom(atom_to_list(Pool) ++ "__bucket")
    end.

poll(Pool) ->
    Name = to_name(Pool),
    case shackle:call(Pool, {{query, <<"select peer from system.peers limit 1">>}, #{}}, 100) of
        {ok, _} ->
            lager:info("marina node ~p recovered, increasing max tickets ", [Pool]),
            gen_server:call(Name, increase_max_ticket);
        _ ->
            schedule_poll(Name)
    end.

checkout_ticket(From, #ticket_bucket{name = Pool, checkedout_ticket = Tickets, bucket = Dict}) ->
    lager:debug("marina accquire ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
    Dict1 =
        case dict:find(From, Dict) of
            error ->
                dict:store(From, 1, Dict);
            {ok, N} ->
                dict:store(From, N + 1, Dict)
        end,
    Dict1.

adjust_max_tickets({ok, _}, #ticket_bucket{max_ticket = Max}) ->
    case Max < ?MAX_TICKET of
        true ->
            Max + 1;
        _ ->
            Max
    end;
adjust_max_tickets({error, timeout}, #ticket_bucket{max_ticket = Max, name = Pool, checkedout_ticket = T}) ->
    NewMax = Max div 2,
    Name = to_name(Pool),
    lager:warning("marina node ~p seens too busy, reduced max tickets to ~p, currently checkedout ~p", [Pool, NewMax, T]),
    case NewMax < 1 of
        %% the pool has been disabled
        true ->
            lager:error("marina no tickets available for node ~p, traffic will not be routed to this node ", [Pool]),
            schedule_poll(Name),
            NewMax;
        false ->
            NewMax
    end;
adjust_max_tickets(_, State) -> State.


schedule_poll(Name) ->
    {ok, Ref} = timer:send_after(100, Name, poll),
    Ref.