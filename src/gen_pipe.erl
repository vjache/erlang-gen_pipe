%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vjache@gmail.com>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(gen_pipe).

-export([start_link/5, 
	 shutdown/2, 
	 send/2, 
	 whereis/1]).

-define(impl, gen_pipe_impl).

%%
%% Types
%%

-type pipe_name() :: atom() |
		     {local, atom()} | 
		     {global, any()} | 
		     {via, atom(), any()}.

-type pipe_ref() :: {node(), pipe_name()}.

%%
%% Callbacks
%%

-callback init( Arg :: any() ) -> {ok, State :: any()}.

-callback terminate(Reason :: any(), State :: any()) -> {ok, State :: any()}.

-callback handle_data (
            In    :: any(), 
            From  :: pipe_ref(), 
            State :: any()) -> 
    {nout | 
     {out, Out :: any()} | 
     {out, Out :: any(), To :: [pipe_ref()]}, 
     State :: any()}.

-callback handle_connect(
            Status  :: up | {down, Reason :: any()},
            Type    :: upstream | downsream, 
            PipeRef :: pipe_ref(), 
            State   :: any()) ->
    {ok, State} | {ok, UpDownstreamOut :: any(), State}.

%%------------------------------------------------------
%% API 
%%------------------------------------------------------

%%
%% @doc
%%

-spec start_link(
        PipeName :: pipe_name(),
        Module   :: atom(), 
        Args     :: any(), 
        UpStreamPipes :: [ pipe_ref()], 
        Opts     :: [any()]) ->
                        {ok, pid()} | {error, any()}.

start_link(PipeName, Module, Args, UpStreamPipes, Opts) ->
    ?impl:start_link(PipeName, Module, Args, UpStreamPipes, Opts).

%%
%% @doc
%%

-spec shutdown(Pipe :: pid() | 
		       pipe_ref(),
	       Reason :: any()) -> ok.

shutdown(Pipe, Reason) ->
    ?impl:shutdown(Pipe, Reason).

send(Pipe, Data) ->
    ?impl:send(Pipe, Data).

whereis(PipeRef) ->
    ?impl:whereis(PipeRef).
