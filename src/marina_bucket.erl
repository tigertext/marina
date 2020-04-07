-module(marina_bucket).
-author("anders").
-compile([{parse_transform, lager_transform}]).
-behaviour(gen_server).

%% API
-export([start_link/1, return_ticket/2, acquire_ticket/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(MAX_TICKET, 1000).
-define(STOP_POLLING_THRESHOLD, 300).

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
    {ok, #{checkedout_ticket => 0, max_ticket => ?MAX_TICKET, name => Name, bucket => dict:new()}}.

-spec return_ticket(atom(), any()) -> ok.
return_ticket(Pool, Reason) ->
    Name = to_name(Pool),
    gen_server:call(Name, {return_ticket, Reason}).

-spec acquire_ticket(atom()) -> ok | error.
acquire_ticket(Pool) ->
    Name = to_name(Pool),
    gen_server:call(Name, acquire_ticket).

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

handle_call(increase_max_ticket, _, State = #{max_ticket := Max}) when Max < ?STOP_POLLING_THRESHOLD ->
    {reply, ok, State#{max_ticket => Max + 1}};
handle_call(increase_max_ticket, _, State = #{tref := TRef})  ->
    stop_polling(TRef),
    {reply, ignored, State#{tref => undefined}};
%% too many tickets checkedout, only allow return, don't allow accquire any more.
handle_call(acquire_ticket, {From, _Ref}, State = #{checkedout_ticket := T, max_ticket := Max, name := Pool}) when T >= Max ->
    lager:warning("cannot aacquire tickets!! pool ~p from ~p, waiting for all tickets to be returned, checkedout ~p, max ~p", [Pool, From, T, Max]),
    {reply, error, State};
%% a pid acquires a ticket, give the ticket to the process and descrease the total number of etickets;
handle_call(acquire_ticket, {From, _Ref}, State) ->
    State1 = checkout_ticket(From, State),
    {reply, ok, State1};
handle_call({return_ticket, Reason}, {From, _Ref},  State = #{name := Pool, checkedout_ticket := Tickets, max_ticket := Max, bucket := Dict}) ->
    lager:debug("release ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
    {Dict1, TicketsToReturn} =
        case dict:find(From, Dict) of
            error ->
                %% some process going to return a ticket but it is not recorded, there must be a leak
                %% TODO: fix possible leak
                {Dict, 0};
            {ok, 1} ->
                {dict:erase(From, Dict), 1};
            {ok, N} when N>1->
                {dict:store(From, N-1, Dict), 1}
        end,
    NewState = adjust_max_tickets(Reason, State),
    {reply, ok, NewState#{checkedout_ticket => (Tickets - TicketsToReturn), bucket => Dict1}}.

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
    {Dict1,  TicketsToReturn} =
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
    list_to_atom(atom_to_list(Pool) ++ "__bucket").

start_polling() ->
    {ok, Ref} = timer:send_interval(300, self(), poll),
    Ref.

stop_polling(Ref) ->
    timer:cancel(Ref).

poll(Pool) ->
    case shackle:call(Pool, {{query, <<"select peer from system.peers limit 1">>}, #{}}, 100) of
        {ok, _} ->
            lager:info("marina node ~p recovered, increasing max tickets ", [Pool]),
            Name = to_name(Pool),
            gen_server:call(Name, increase_max_ticket);
        _ ->
            ok
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

adjust_max_tickets({ok, _}, State = #{max_ticket := Max}) ->
    case Max < ?MAX_TICKET of
        true ->
            State#{max_ticket => Max + 1};
        _ ->
            State
    end;
adjust_max_tickets({error, timeout}, State = #{max_ticket := Max, name := Pool, checkedout_ticket := T}) ->
    NewMax = Max div 2,
    lager:warning("marina node ~p seens too busy, reduced max tickets to ~p, currently checkedout ~p", [Pool, NewMax, T]),
    case NewMax < 1 of
        %% the pool has been disabled
        true ->
            lager:error("marina no tickets available for node ~p, traffic will not be routed to this node ", [Pool]),
            Ref = start_polling(),
            State#{max_ticket => NewMax, tref => Ref};
        false ->
            State#{max_ticket => NewMax}
    end;
adjust_max_tickets(_, State) -> State.


