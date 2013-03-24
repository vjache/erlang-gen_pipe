%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vjache@gmail.com>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(std_pipe).

-behavior(gen_pipe).

% Export 'gen_pipe' callbacks.
-export([init/1,
	 terminate/2,
	 handle_connect/4,
	 handle_data/3]).

% Export API
-export([start_link_foreach/3, start_link_foreach/4,
	 start_link_fold/4, start_link_fold/5,
	 start_link_filter/3, start_link_filter/4,
	 start_link_map/3, start_link_map/4,
	 start_link_mapfoldfilter/4, start_link_mapfoldfilter/5]).

-type map_fun()           :: fun( ( any() ) -> any() ).
-type filter_fun()        :: fun( ( any() ) -> boolean() ).
-type fold_fun()          :: fun( ( Value :: any(), AccIn :: any() ) -> AccOut :: any() ).
-type foreach_fun()       :: fun( ( any() ) -> ok ).
-type mapfoldfilter_fun() :: fun( ( ValueIn :: any(), AccIn :: any() ) -> 
					{out, ValueOut :: any(), AccOut :: any()} |
					{nout, AccOut :: any()}).

-type pipe_refs()      :: [gen_pipe:pipe_ref()].
-type opts()           :: [{accept_info, boolean()}].
-type ret()            :: {ok, pid()}.

-define(ECHO(Msg), io:format("(~p|~p|~p) : ~p~n", [?MODULE, self(), ?LINE, Msg])).

-record(foreach, {}).
-record(fold,    {acc}).
-record(map,     {}).
-record(filter,  {}).
-record(mapfoldfilter, {acc}).

-record(state, 
	{func :: fun(), accept_info = false :: boolean(), 
	 specific :: #foreach{} | 
		     #fold{} | 
		     #map{} | 
		     #filter{} |
		     #mapfoldfilter{} }).

%%-----------------------------------------------
%% API functions
%%-----------------------------------------------

%%
%% @doc 
%%
start_link_mapfoldfilter(
  PipeName, 
  MapFoldFilterFun, Acc0, UpstPipeRefs) ->
    start_link_mapfoldfilter(
      PipeName, 
      MapFoldFilterFun, Acc0, UpstPipeRefs, []).

-spec start_link_mapfoldfilter(
	PipeName         :: gen_pipe:pipe_name(), 
	MapFoldFilterFun :: mapfoldfilter_fun(), 
	Acc0             :: any(), 
	UpstPipeRefs     :: pipe_refs(), 
	Opts             :: opts() ) -> ret().

start_link_mapfoldfilter(
  PipeName, 
  MapFoldFilterFun, Acc0, UpstPipeRefs, Opts) when is_function(MapFoldFilterFun, 2), 
						   is_list(UpstPipeRefs) ->
    gen_pipe:start_link(
      PipeName, ?MODULE, {mapfoldfilter, MapFoldFilterFun, Acc0, Opts}, 
      UpstPipeRefs, []).

%%
%% @doc 
%%
start_link_map(
  PipeName, 
  MapFun, UpstPipeRefs) ->
    start_link_map(
      PipeName, 
      MapFun, UpstPipeRefs, []).

-spec start_link_map(
	PipeName         :: gen_pipe:pipe_name(), 
	MapFun           :: map_fun(), 
	UpstPipeRefs     :: pipe_refs(), 
	Opts             :: opts() ) -> ret().

start_link_map(
  PipeName, 
  MapFun, UpstPipeRefs, Opts) when is_function(MapFun, 1), is_list(UpstPipeRefs) ->
    gen_pipe:start_link(
      PipeName, ?MODULE, {map, MapFun, Opts}, 
      UpstPipeRefs, []).

%%
%% @doc 
%%
start_link_filter(
  PipeName, 
  FilterFun, UpstPipeRefs) ->
    start_link_filter(
      PipeName, 
      FilterFun, UpstPipeRefs, []).

-spec start_link_filter(
	PipeName         :: gen_pipe:pipe_name(), 
	FilterFun        :: filter_fun(), 
	UpstPipeRefs     :: pipe_refs(), 
	Opts             :: opts() ) -> ret().

start_link_filter(
  PipeName, 
  FilterFun, UpstPipeRefs, Opts) when is_function(FilterFun, 1), is_list(UpstPipeRefs) ->
    gen_pipe:start_link(
      PipeName, ?MODULE, {filter, FilterFun, Opts}, 
      UpstPipeRefs, Opts).

%%
%% @doc 
%%
start_link_foreach(
  PipeName, 
  ForeachFun, UpstPipeRefs) ->
    start_link_foreach(
      PipeName, 
      ForeachFun, UpstPipeRefs, []).

-spec start_link_foreach(
	PipeName         :: gen_pipe:pipe_name(), 
	ForeachFun       :: foreach_fun(), 
	UpstPipeRefs     :: pipe_refs(), 
	Opts             :: opts() ) -> ret().

start_link_foreach(
  PipeName, 
  ForeachFun, UpstPipeRefs, Opts) when is_function(ForeachFun, 1), is_list(UpstPipeRefs) ->
    gen_pipe:start_link(
      PipeName, ?MODULE, {foreach, ForeachFun, Opts}, 
      UpstPipeRefs, Opts).

%%
%% @doc 
%%
start_link_fold(
  PipeName, 
  FoldFun, Acc, UpstPipeRefs) ->
    start_link_fold(
      PipeName, 
      FoldFun, Acc, UpstPipeRefs, []).

-spec start_link_fold(
	PipeName         :: gen_pipe:pipe_name(), 
	FoldFun          :: fold_fun(), 
	UpstPipeRefs     :: pipe_refs(), 
	Opts             :: opts() ) -> ret().

start_link_fold(
  PipeName, 
  FoldFun, Acc, UpstPipeRefs, Opts) when is_function(FoldFun, 2), is_list(UpstPipeRefs) ->
    gen_pipe:start_link(
      PipeName, ?MODULE, {fold, FoldFun, Acc, Opts}, 
      UpstPipeRefs, Opts).

%%-----------------------------------------------
%% 'gen_pipe' behaviour callbacks
%%-----------------------------------------------

get_opt_accept_info(Opts) ->
    V = proplists:get_value(accept_info, Opts, false),
    true = is_boolean(V),
    V.

init({foreach, ForeachFun, Opts} = Spec) ->
    ?ECHO({init, Spec}),
    {ok, #state{ func        = ForeachFun, 
		 accept_info = get_opt_accept_info(Opts),
		 specific    = #foreach{} } };

init({fold, FoldFun, Acc0, Opts} = Spec) ->
    ?ECHO({init, Spec}),
    {ok, #state{ func = FoldFun,
		 accept_info = get_opt_accept_info(Opts),
		 specific = #fold{ acc = Acc0 } } };

init({filter, FilterFun, Opts} = Spec) ->
    ?ECHO({init, Spec}),
    {ok, #state{ func = FilterFun, 
		 accept_info = get_opt_accept_info(Opts),
		 specific = #filter{} } };

init({map, MapFun, Opts} = Spec) ->
    ?ECHO({init, Spec}),
    {ok, #state{ func = MapFun, 
		 accept_info = get_opt_accept_info(Opts),
		 specific = #map{} } };

init({mapfoldfilter, MapFoldFilterFun, Acc, Opts} = Spec) ->
    ?ECHO({init, Spec}),
    {ok, #state{ func = MapFoldFilterFun, 
		 accept_info = get_opt_accept_info(Opts),
		 specific = #mapfoldfilter{ acc = Acc } } }.


terminate(_Reason, State) ->
    {ok, State}.

handle_connect(up, downstream, _PipeRef, #state{specific = #fold{acc = Acc} } = State) ->
    {ok, Acc, State};

handle_connect(_Status, _Type, _PipeRef, State) ->
    ?ECHO({handle_connect, _Status, _Type, _PipeRef, State}),
    {ok, State}.

handle_data(_Info, 
	    info, #state{accept_info = false} = State) ->
    ?ECHO({unexpected_info, _Info}),
    { nout, State };

handle_data(Data, _UpstPipeRef, 
	    #state{func = Func, specific = #map{} } = State) ->
    Data1 = Func(Data),
    { {out, Data1}, State };

handle_data(Data, _UpstPipeRef, 
	    #state{func = Func, specific = #filter{} } = State) ->
    case Func(Data) of
	true  -> { {out, Data}, State };
	false -> { nout, State}
    end;

handle_data(Data, _UpstPipeRef, 
	    #state{func = Func, specific = #mapfoldfilter{ acc = Acc} = Sp } = State) ->
    State1 = fun(Acc_) ->
		     State#state{ specific = Sp#mapfoldfilter{ acc = Acc_ }}
	     end,
    case Func(Data, Acc) of
	{out, Data1, Acc1}  -> { {out, Data1}, State1(Acc1) };
	{nout, Acc1}        -> { nout, State1(Acc1)}
    end;

handle_data(Data, _UpstPipeRef, 
	    #state{func = Func, specific = #foreach{} } = State) ->
    Func(Data),
    { {out, Data}, State };

handle_data(Data, _UpstPipeRef, 
	    #state{func = Func, 
		   specific = #fold{ acc = Acc } = F } = State) ->
    Acc1 = Func(Data, Acc),
    { {out, Acc1}, State#state{ specific = F#fold{ acc = Acc1} } }.

    


