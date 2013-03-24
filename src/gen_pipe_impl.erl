%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vjache@gmail.com>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(gen_pipe_impl).

-behaviour(gen_server).

%% 'gen_pipe' API impl
-export([start_link/5, whereis/1, send/2, shutdown/2]).

%% gen_server callbacks
-export([init/1, 
	 handle_call/3, 
	 handle_cast/2, 
	 handle_info/2,
	 terminate/2, 
	 code_change/3]).

-export([subscribe_for_online/1]).

-define(SERVER, ?MODULE). 

-record(state, {pname              :: gen_pipe:pipe_name(),
		pref               :: gen_pipe:pipe_ref(),
		pstate             :: any(),
		mod                :: atom(),
		upst_online   = [] :: { gen_pipe:pipe_ref(), 
					pid() },
		upst_offline  = [] :: gen_pipe:pipe_ref(),
		downst_online = dict:new(),
		sub_reqs      = [] :: {gen_pipe:pipe_ref(), 
				       reference()},
		opts
	       }).

-record(pipe_opts, {await_upstream_connect = async}).

-define(NOTIFY(Msg), (catch gen_event:notify(gen_pipe_events, Msg)) ). 

%%%===================================================================
%%% API
%%%===================================================================

start_link(PipeName, Module, Args, UpStreamPipes, Opts) 
  when is_atom(Module), 
       is_list(UpStreamPipes), 
       is_list(Opts) ->
    {PipeOpts, GenServerOpts} = parse_opts(Opts),
    {AwaitFun, NotifyFun} = 
	case PipeOpts#pipe_opts.await_upstream_connect of
	    sync -> 
		Marker = make_ref(),
		Self = self(),
		{fun()-> receive 
			     Marker -> 
				 ok 
			 after 5000 -> 
				 exit({failed_connect_upstream, UpStreamPipes}) 
			 end 
		 end,
		 fun(_) -> Self ! Marker end};
	    async -> 
		{fun()-> ok end, fun(_)-> ok end};
	    NF when is_function(NF, 1) -> 
		{fun()-> ok end, NF}
	end,
    PipeOpts1 = PipeOpts#pipe_opts{await_upstream_connect = NotifyFun},
    case gen_server:start_link(
	   PipeName, ?MODULE, 
	   {PipeName, Module, Args, UpStreamPipes, PipeOpts1}, GenServerOpts) of
	{ok, _} = R -> AwaitFun(),R;
	Err         -> Err
    end.

parse_opts(Opts) ->
    parse_opts(#pipe_opts{}, [], Opts).

parse_opts(PO, GO, [{await_upstream_connect, Val} | Tail]) ->
    parse_opts(PO#pipe_opts{await_upstream_connect = Val}, GO, Tail);
parse_opts(PO, GO, [H|Tail]) ->
    parse_opts(PO, [H | GO], Tail);
parse_opts(PO, GO, []) ->
    {PO, GO}.

shutdown(PipeRef, Reason) ->
    ok = gen_server:call(
	   ?MODULE:whereis(PipeRef), 
	   {shutdown, Reason}).

send(Pipe, Data) when is_pid(Pipe) ->
    Pipe ! Data;
send(Pipe, Data) ->
    ?MODULE:whereis(Pipe) ! Data.

whereis(Pid) when is_pid(Pid) ->
    Pid;
whereis(PipeRef) ->
    case resolve_pid(PipeRef) of
	undefined -> exit({no_pipe, PipeRef});
	Pid when is_pid(Pid)-> Pid
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({PipeName, Module, Args, UpStreamPipes, PipeOpts}) ->
    try Module:init(Args) of
	{ok, PState} ->
	    Watcher = get_watcher(),
	    Watcher:subscribe_for_online(UpStreamPipes),
	    case UpStreamPipes of
		[] -> (PipeOpts#pipe_opts.await_upstream_connect)(PipeName);
		_  -> ok
	    end,
	    {ok, 
	     #state{pname        = PipeName,
                    pref         = gref(PipeName),
		    pstate       = PState, 
		    mod          = Module, 
		    upst_offline = UpStreamPipes,
		    opts = PipeOpts}}
    catch
	_:Reason -> {stop, Reason}
    end.

get_watcher() ->
    ?MODULE.

subscribe_for_online(_PipesRefs) ->
    subscribe_for_online_(0).
subscribe_for_online_(N) ->
    erlang:send_after(
      if N == 0 -> 0; true -> 100 end, 
      self(), 
      {try_resolve, 1}).
handle_call({shutdown, Reason}, _From, _State) ->
    {stop, Reason, ok, _State};
handle_call(_Request, _From, _State) ->
    exit({unexpected, _Request}).

handle_cast({'$gen_pipe.push', PipeRefFrom, DataIn}, #state{} = State) ->
    handle_data(PipeRefFrom, DataIn, State).

push_data(Pid, Data, From) ->
    gen_server:cast(Pid, {'$gen_pipe.push', From, Data}).

gref({local, Atom}) ->
    {node(), {local, Atom} };
gref({global, _} = G) ->
    {node(), G};
gref({via, _, _} = Via) ->
    {node(), Via};
gref(Atom) when is_atom(Atom) ->
    {node(), {local, Atom} }.

resolve_pid({Node, Name }) when Node =:= node(), is_atom(Name) ->
    erlang:whereis(Name);
resolve_pid({Node, {local, Name} }) when Node =:= node() ->
    resolve_pid({Node, Name});
resolve_pid({Node, {local, Name} }) ->
    case rpc:call(Node, erlang, whereis, [Name]) of
	{badrpc, _Reason} ->
	    undefined;
	Value -> Value
    end;
resolve_pid({_Node, {global, Name} }) ->
    global:whereis(Name);
resolve_pid({Node, {via, Mod, Name}}) ->
    case rpc:call(Node, Mod, whereis_name, [Name]) of
	{badrpc, _Reason} ->
	    undefined;
	Value -> Value
    end.

%% Its time to try to resolve pids of upstream pipes
handle_info({try_resolve, N}, 
	    #state{upst_offline = UpstOffline} = State) ->
    case lists:member(
      false, [case resolve_pid(PRef) of
		  undefined ->
		      false;
		  Pid ->
		      self() ! {'PROCESS_REGISTERED', {PRef, Pid} }
	      end || PRef <- UpstOffline]) of
	true -> 
	    subscribe_for_online_(N+1);
	false ->
	    ok
    end,
    {noreply, State };
%% Upstream pipe appeared
handle_info({'PROCESS_REGISTERED', {PRef, Pid} },
	    #state{pname = PName, sub_reqs = SR} = State) ->
    %% Send subscription request to upstream pipe
    ReqId = make_ref(),
    Pid ! {'$gen_pipe.subscribe.req', gref(PName), self(), ReqId},
    {noreply, State#state{sub_reqs = [{PRef, Pid, ReqId} | SR]} };
%% Handle subscription request from downstream pipe
handle_info({'$gen_pipe.subscribe.req', DownstPref, DownstPid, ReqId},
	    #state{pref = Pref, pstate = PState, mod = PMod, downst_online = DownstOnline} = State) ->
    erlang:monitor(process, DownstPid),
    DownstPid ! {'$gen_pipe.subscribe.ack', ReqId},
    case PMod:handle_connect(up, downstream, DownstPref, PState) of
	{ok, PState1} -> ok;
	{ok, DownstData, PState1} ->
	    PFrom = Pref,
	    push_data(DownstPid, DownstData, PFrom)
    end,
    {noreply, State#state{pstate = PState1, downst_online = dict:store(DownstPref,DownstPid, DownstOnline)} };
%% Handle subscription acknowledgement
handle_info({'$gen_pipe.subscribe.ack', ReqId},
	    #state{pname        = PName,
		   pref         = Pref,
		   pstate       = PState, 
		   mod          = PMod, 
		   upst_online  = UpstOnline,
		   upst_offline = UpstOffline,
		   sub_reqs     = SR,
		   opts = #pipe_opts{await_upstream_connect = AUC}
		  } = State) ->
    {value, {UpstPref, Pid, ReqId}, SR1} = lists:keytake(ReqId, 3, SR),
    erlang:monitor(process, Pid),
    UpstOnline1   = [Upst={UpstPref, Pid} | UpstOnline],
    UpstOffline1  = lists:delete(UpstPref, UpstOffline),
    {ok, PState1} = PMod:handle_connect(up, upstream, UpstPref, PState),
    ?NOTIFY({connected_to_upstream,     Pref, Upst}),
    case UpstOffline1 of
	[] ->
	    AUC(PName),
	    ?NOTIFY({connected_to_all_upstream, Pref, UpstOnline1});
	_  -> ok
    end,
    {noreply, 
     State#state{pstate       = PState1,
		 upst_online  = UpstOnline1, 
		 upst_offline = UpstOffline1,
		 sub_reqs     = SR1
		} };
%% Some process down. This may be an up and/or down stream pipe or something else
handle_info({'DOWN', _MonRef, process, Pid, Reason}, 
	    #state{pstate       = PState,
		   mod          = PMod, 
		   upst_online  = UpstOnline,
		   upst_offline = UpstOffline,
		   downst_online = DownstOnline} = State) ->
    RemoveDownst = 
	fun(State0) ->
		% FIXIT: We must handle the case when more than one entries share the same pid.
		case dict:fold(fun(K,V,_Acc) ->
				       if V == Pid ->
					       {true, K};
					  true ->
					       _Acc
				       end
			       end, 
			       false, 
			       DownstOnline) of
		    {true, Pref} ->
			{ok, PState1} = PMod:handle_connect(
					  {down, Reason}, downstream, 
					  Pref, State0#state.pstate),
			State0#state{pstate        = PState1, 
				     downst_online = dict:erase(Pref, DownstOnline) };
		    false ->
			State0
	     end
	end,
    {noreply, 
     case lists:keytake(Pid, 2, UpstOnline) of
	 {value, {PRef, Pid}, UpstOnline1} -> 
	     {ok, PState1} = PMod:handle_connect(
			       {down, Reason}, upstream, PRef, PState),
	     RemoveDownst(
	       State#state{pstate       = PState1, 
			   upst_online  = UpstOnline1, 
			   upst_offline = [PRef | UpstOffline]});
	 false ->
	     RemoveDownst(State)
     end};
handle_info(InfoMsg, State) ->
    handle_data(info, InfoMsg, State).

handle_data(PipeRefFrom, DataIn, 
	    #state{pref = Pref, pstate = PState, 
		   mod = Mod, downst_online = DownstOnline} = State) ->
    try Mod:handle_data(DataIn, PipeRefFrom, PState) of
	{Out, PState1} ->
    	    PFrom = Pref,
	    case Out of
		nout -> [];
		{out,  DataOut} -> 
		    dict:fold(fun(_,Pid,_)->
				      push_data(Pid, DataOut, PFrom)
			      end, ok, DownstOnline);
		{out,  DataOut, PipeRefsTo } -> 
		    [case dict:find(PipeRefTo, DownstOnline) of
			 {ok, Pid} ->
			     push_data(Pid, DataOut, PFrom);
			 error -> ok
		     end || PipeRefTo <- PipeRefsTo]
	    end,
	    {noreply, State#state{ pstate = PState1 } }
    catch
	_:Reason ->
	    {stop, Reason}
    end.
    





terminate(Reason, #state{pstate = PState,
			 mod    = PMod} ) ->
    PMod:terminate(Reason, PState),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
