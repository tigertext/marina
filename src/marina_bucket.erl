-module(marina_bucket).
-author("anders").

-behaviour(gen_server).

%% API
-export([start_link/1, return_ticket/1, accquire_ticket/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
    code_change/3]).

-define(SERVER, ?MODULE).

-define(DEFAULT_TICKET, 10).

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
    {ok, #{ticket => ?DEFAULT_TICKET, name => Name, bucket => dict:new()}}.

-spec return_ticket(atom()) -> ok.
return_ticket(Pool) ->
    Name = to_name(Pool),
    gen_server:call(Name, return_ticket).

-spec accquire_ticket(atom()) -> ok | error.
accquire_ticket(Pool) ->
    Name = to_name(Pool),
    gen_server:call(Name, accquire_ticket).

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
handle_call(accquire_ticket, {From, _Ref}, State = #{ticket := T, name := Pool}) when T < 1 ->
    shackle_utils:warning_msg(?MODULE, "cannot aacquire tickets!! pool ~p from ~p", [Pool, From]),
    {reply, error, State};
%% a pid accquire a ticket, give the ticket to the process and descrease the total number of etickets;
handle_call(accquire_ticket, {From, _Ref}, State = #{name := Pool, ticket := Tickets, bucket := Dict}) ->
    shackle_utils:warning_msg(?MODULE, "accquire ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
    Dict1 =
    case dict:find(From, Dict) of
        error ->
            dict:store(From, 1, Dict);
        {ok, N} ->
            dict:store(From, N+1, Dict)
    end,
    monitor(process, From),
    {reply, ok, State#{ticket => (Tickets - 1), bucket => Dict1}};
handle_call(return_ticket, {From, _Ref},  State = #{name := Pool, ticket := Tickets, bucket := Dict}) ->
    shackle_utils:warning_msg(?MODULE, "release ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
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
    {reply, ok, State#{ticket => (Tickets + TicketsToReturn), bucket => Dict1}}.


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
handle_info({'DOWN', _MonitorRef, process, From, _Info}, State = #{name := Pool, ticket := Tickets, bucket := Dict}) ->
    %% process down
    shackle_utils:warning_msg(?MODULE, "release ticket from pool ~p, current tickets: ~p", [Pool, Tickets]),
    {Dict1,  TicketsToReturn} =
        case dict:find(From, Dict) of
            error ->
                %% all tickets for the process have been returned.
                {Dict, 0};
            {ok, N} ->
                {dict:erase(From, Dict), N}
        end,
    {noreply, State#{ticket => (Tickets + TicketsToReturn), bucket => Dict1}};
    
handle_info(_Info, State = #{}) ->
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