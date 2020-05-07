-module(marina_tester).

-export([test/2]).


test(Speed, Rounds) ->
    % F = fun() -> marina_bucket:acquire_ticket('marina_127.0.0.1'), marina_bucket:return_ticket('marina_127.0.0.1', {ok, normal}) end,
    F = fun() -> marina:query(<<"select * from system.peers limit 1">>, #{}) end,
    Result = fun() -> gather_results() end,
    Pid = spawn(Result),
    F1 = fun() -> {V, _} = timer:tc(F), Pid!V end,
    send(F1, Speed, Rounds).

send(_F, _, 0) ->
    ok;
send(F, Num, N) ->
    Before = os:timestamp(),
    [
        begin
            spawn(F)
        end || _<- lists:seq(1, Num)],
    After = os:timestamp(),
    Diff = timer:now_diff(After, Before),
    Sleep = (1000000 - Diff) div 1000,
    timer:sleep(Sleep),
    After1 = os:timestamp(),
    Diff1 = timer:now_diff(After1, Before),
    % ct:pal("time used ~p for reqeusts", [Diff1]),
    send(F, Num, N-1).


gather_results() ->
    Before = os:timestamp(),
    gather_results(Before, []).

gather_results(StartTime, Result) ->
    After = os:timestamp(),
    Diff = timer:now_diff(After, StartTime),
    case {Diff >= 1000000, Result}  of
        {true, []} ->
            ct:pal("gather_results stopped");
        {true, _} ->
            spawn(fun() -> result(Diff, Result) end),
            Before = os:timestamp(),
            gather_results(Before, []);
        _ ->
            receive
                T ->
                    gather_results(StartTime, [T|Result])
            after 0 ->
                gather_results(StartTime, Result)
            end
    end.

result(_Diff, []) -> ok;
result(Diff, Result) ->
    Max = lists:max(Result),
    Min = lists:min(Result),
    Average = lists:sum(Result)/length(Result),
    V = io_lib:format("~p\t~p\t~p\t~p\t~p\t", [Diff, Max, Min, Average, length(Result)]),
    io:format("~s~n", [V]).

