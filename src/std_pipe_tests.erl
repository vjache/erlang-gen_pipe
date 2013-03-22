%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vjache@gmail.com>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(std_pipe_tests).

-include_lib("eunit/include/eunit.hrl").

map_test()     ->
    PipeName         = {local, map_pipe1},
    MapFun           = fun(I)-> {mapped, I} end, 
    UpstreamPipeRefs = [],
    {ok, Pid} = std_pipe:start_link_map(
      PipeName, MapFun, UpstreamPipeRefs, [{accept_info, true}]),
    {ok, Pid1} = start_helper_pipe(PipeName),
    timer:sleep(10),
    Pid ! 1,
    ?assertEqual({mapped, 1}, receive_result()),
    ?assert(is_process_alive(Pid)  == true),
    ?assert(is_process_alive(Pid1) == true),
    ok = gen_pipe:shutdown(Pid, normal),
    ok = gen_pipe:shutdown(Pid1,normal),
    ?assert(is_process_alive(Pid)  == false),
    ?assert(is_process_alive(Pid1) == false).

fold_test()    ->
    ok.

foreach_test() ->
    ok.

start_helper_pipe(UpstPipeName) ->
    RetPid = self(),
    {ok, _} = std_pipe:start_link_foreach(
		{local, list_to_atom(
			  lists:flatten(io_lib:format("~p_helper", [UpstPipeName])))},
		fun(I) ->
			RetPid ! I
		end, [{node(), UpstPipeName}]).
    
receive_result() ->
    receive
	I ->
	    I
    after 50 ->
	exit(no_result)
    end.
