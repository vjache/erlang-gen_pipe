%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vvorobyov@hydra>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%% Created :  6 Mar 2013 by Vyacheslav Vorobyov <vvorobyov@hydra>
%%%-------------------------------------------------------------------
-module(test_pipe).

-behavior(gen_pipe).

%% API
-export([start_link/2]).
-export([init/1,
	 terminate/2,
	 handle_connect/4,
	 handle_data/3]).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% @spec
%% @end
%%--------------------------------------------------------------------

start_link(PipeName, UpstPipes ) ->
    gen_pipe:start_link(PipeName, ?MODULE, 0, UpstPipes, []).

%%%===================================================================
%%% Internal functions
%%%===================================================================

init(Arg) ->
    io:format("(~p) : init(~p)~n", [self(), Arg]),
    {ok, 0}.

terminate(_Reason, State) ->
    {ok, State}.

handle_connect(Status, Type, PipeRef, State) ->
    io:format("(~p) : handle_connect(~p, ~p, ~p, ~p)~n", [self(), Status, Type, PipeRef, State]),
    {ok, State}.

handle_data(In, From, State) ->
    io:format("(~p) : handle_data(~p, ~p, ~p)~n", [self(), In, From, State]),
    { {out, State}, State + 1}.


