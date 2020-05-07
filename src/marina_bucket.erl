-module(marina_bucket).
-author("anders").
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/4, return_ticket/2, acquire_ticket/1, stop/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

%%%===================================================================
%%% API
%%%===================================================================

%% @doc Spawns the server and registers the local name (unique)
-spec(start_link(any(), pos_integer(), float(), pos_integer()) ->
    {ok, Pid :: pid()} | ignore | {error, Reason :: term()}).
start_link(NodeId, PeerMaxTickets, PeerTimeoutDescreaseFactor, PeerStopPollingThreshold) ->
    Name = to_name(NodeId),
    gen_server:start_link({local, Name}, ?MODULE, [NodeId, PeerMaxTickets, PeerTimeoutDescreaseFactor, PeerStopPollingThreshold], []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%% @private
%% @doc Initializes the server
-spec(init(Args :: term()) ->
    {ok, State :: #{}} | {ok, State :: #{}, timeout() | hibernate} |
    {stop, Reason :: term()} | ignore).
init([Name, PeerMaxTickets, PeerTimeoutReduceFactor, PeerStopPollingThreshold]) ->
    {ok, #{checkedout_ticket => 0, max_ticket => PeerMaxTickets, timeout_reduce_factor => PeerTimeoutReduceFactor,
        stop_polling_threshold => PeerStopPollingThreshold, configured_max_ticket => PeerMaxTickets,
        name => Name, bucket => dict:new()}}.

-spec return_ticket(atom(), any()) -> ok.
return_ticket(Pool, Reason) ->
    Name = to_name(Pool),
    gen_server:call(Name, {return_ticket, Reason}).

-spec acquire_ticket(atom()) -> ok | error.
acquire_ticket(Pool) ->
    Name = to_name(Pool),
    gen_server:call(Name, acquire_ticket).

stop(Pool) ->
    Name = to_name(Pool),
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

handle_call(increase_max_ticket, _, State = #{max_ticket := Max, stop_polling_threshold := Stop_Polling_Threshold}) when Max < Stop_Polling_Threshold ->
    schedule_poll(self()),
    {reply, ok, State#{max_ticket => Max + 1}};
handle_call(increase_max_ticket, _, State) ->
    {reply, ignored, State#{tref => undefined}};
%% too many tickets checkedout, only allow return, don't allow accquire any more.
handle_call(acquire_ticket, {From, _Ref}, State = #{checkedout_ticket := T, max_ticket := Max, name := Pool}) when T >= Max ->
    lager:warning("cannot acquire tickets!! pool ~p from ~p, waiting for all tickets to be returned, checkedout ~p, max ~p", [Pool, From, T, Max]),
    {reply, error, State};
%% a pid acquires a ticket, give the ticket to the process and descrease the total number of etickets;
handle_call(acquire_ticket, {From, _Ref}, State) ->
    State1 = checkout_ticket(From, State),
    {reply, ok, State1};
handle_call({return_ticket, Reason}, {From, _Ref}, State = #{name := Pool, checkedout_ticket := Tickets, max_ticket := Max, bucket := Dict}) ->
    lager:debug("release ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
    {Dict1, TicketsToReturn} =
        case dict:find(From, Dict) of
            error ->
                lager:warning("cannot find process for pool ~p ~p in dict ~p ", [Pool, From, Dict]),
                {Dict, 0};
            {ok, 1} ->
                {dict:erase(From, Dict), 1};
            {ok, N} when N > 1 ->
                {dict:store(From, N - 1, Dict), 1}
        end,
    NewState = adjust_max_tickets(Reason, State),
    {reply, ok, NewState#{checkedout_ticket => (Tickets - TicketsToReturn), bucket => Dict1}};
handle_call(stop, _From, State) ->
    {stop, normal, State}.

%% @private
%% @doc Handling cast messages
-spec(handle_cast(Request :: term(), State :: #{}) ->
    {noreply, NewState :: #{}} |
    {noreply, NewState :: #{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #{}}).
handle_cast(_Request, State = #{}) ->
    {noreply, State}.

%% @private
%% @doc Handling all non call/cast messages
-spec(handle_info(Info :: timeout() | term(), State :: #{}) ->
    {noreply, NewState :: #{}} |
    {noreply, NewState :: #{}, timeout() | hibernate} |
    {stop, Reason :: term(), NewState :: #{}}).
handle_info({'DOWN', _MonitorRef, process, From, _Info}, State = #{name := Pool, checkedout_ticket := Tickets, bucket := Dict}) ->
    %% process down
    lager:info("marina release ticket from pool ~p, process down ~p current tickets: ~p", [Pool, From, Tickets]),
    {Dict1, TicketsToReturn} =
        case dict:find(From, Dict) of
            error ->
                %% all tickets for the process have been returned.
                {Dict, 0};
            {ok, N} ->
                {dict:erase(From, Dict), N}
        end,
    {noreply, State#{checkedout_ticket => (Tickets - TicketsToReturn), bucket => Dict1}};

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

checkout_ticket(From, State = #{name := Pool, checkedout_ticket := Tickets, bucket := Dict}) ->
    lager:debug("marina accquire ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
    Dict1 =
        case dict:find(From, Dict) of
            error ->
                dict:store(From, 1, Dict);
            {ok, N} ->
                dict:store(From, N + 1, Dict)
        end,
    monitor(process, From),
    State#{checkedout_ticket => (Tickets + 1), bucket => Dict1}.

adjust_max_tickets({ok, _}, State = #{max_ticket := Max, configured_max_ticket := PeerMaxTickets}) ->
    case Max < PeerMaxTickets of
        true ->
            State#{max_ticket => Max + 1};
        _ ->
            State
    end;
adjust_max_tickets({error, timeout}, State = #{max_ticket := Max, name := Pool, checkedout_ticket := T, timeout_reduce_factor := Timeout_Reduce_Factor}) ->
    NewMax = Max div Timeout_Reduce_Factor,
    Name = to_name(Pool),
    lager:warning("marina node ~p seens too busy, reduced max tickets to ~p, currently checkedout ~p", [Pool, NewMax, T]),
    case NewMax < 1 of
        %% the pool has been disabled
        true ->
            lager:error("marina no tickets available for node ~p, traffic will not be routed to this node ", [Pool]),
            schedule_poll(Name),
            State#{max_ticket => NewMax};
        false ->
            State#{max_ticket => NewMax}
    end;
adjust_max_tickets(_, State) -> State.


schedule_poll(Name) ->
    {ok, Ref} = timer:send_after(100, Name, poll),
    Ref.