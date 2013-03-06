%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vjache@gmail.com>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------
-module(gen_pipe_impl).

-behaviour(gen_server).

%% API
-export([start_link/5]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
	 terminate/2, code_change/3, subscribe_for_online/1]).

-define(SERVER, ?MODULE). 

-record(state, {pname              :: gen_pipe:pipe_name(),
		pstate             :: any(),
		mod                :: atom(),
		upst_online   = [] :: { gen_pipe:pipe_ref(), pid() },
		upst_offline  = [] :: gen_pipe:pipe_ref(),
		downst_online = dict:new(),
		sub_reqs      = [] :: {gen_pipe:pipe_ref(), reference()}
	       }).

%%%===================================================================
%%% API
%%%===================================================================

start_link(PipeName, Module, Args, UpStreamPipes, Opts) 
  when is_atom(Module), 
       is_list(UpStreamPipes), 
       is_list(Opts) ->
    gen_server:start_link(
      PipeName, ?MODULE, 
      {PipeName, Module, Args, UpStreamPipes}, Opts).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({PipeName, Module, Args, UpStreamPipes}) ->
    try Module:init(Args) of
	{ok, PState} ->
	    Watcher = get_watcher(),
	    Watcher:subscribe_for_online(UpStreamPipes),
	    {ok, 
	     #state{pname         = PipeName,
		    pstate       = PState, 
		    mod          = Module, 
		    upst_offline = UpStreamPipes}}
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

handle_call(_Request, _From, _State) ->
    exit({unexpected, _Request}).

handle_cast({'$gen_pipe.push', PipeRefFrom, DataIn}, 
	    #state{pname = PName, pstate = PState, 
		   mod = Mod, downst_online = DownstOnline} = State) ->
    try Mod:handle_data(DataIn, PipeRefFrom, PState) of
	{Out, PState1} ->
    	    PFrom = gref(PName),
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

push_data(Pid, Data, From) ->
    send(Pid, {'$gen_pipe.push', From, Data}).

send(Pid, Msg) when is_pid(Pid) ->
    gen_server:cast(Pid, Msg).

gref({local, Atom}) ->
    {node(), {local, Atom} };
gref({global, _} = G) ->
    {node(), G};
gref({via, _, _} = Via) ->
    {node(), Via};
gref(Atom) when is_atom(Atom) ->
    {node(), {local, Atom} }.

resolve_pid({Node, Name }) when Node =:= node(), is_atom(Name) ->
    whereis(Name);
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
	    #state{pstate = PState, mod = PMod, downst_online = DownstOnline} = State) ->
    erlang:monitor(process, DownstPid),
    DownstPid ! {'$gen_pipe.subscribe.ack', ReqId},
    {ok, PState1} = PMod:handle_connect(up, downstream, DownstPref, PState),
    {noreply, State#state{pstate = PState1, downst_online = dict:store(DownstPref,DownstPid, DownstOnline)} };
%% Handle subscription acknowledgement
handle_info({'$gen_pipe.subscribe.ack', ReqId},
	    #state{pstate       = PState, 
		   mod          = PMod, 
		   upst_online  = UpstOnline,
		   upst_offline = UpstOffline,
		   sub_reqs = SR
		  } = State) ->
    {value, {Pref, Pid, ReqId}, SR1} = lists:keytake(ReqId, 3, SR),
    erlang:monitor(process, Pid),
    UpstOnline1   = [{Pref, Pid} | UpstOnline],
    UpstOffline1  = lists:delete(Pref, UpstOffline),
    {ok, PState1} = PMod:handle_connect(up, upstream, Pref, PState),
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
				    {down, Reason}, downstream, Pref, State0#state.pstate),
			State0#state{pstate        = PState1, 
				     downst_online = dict:erase(Pref, DownstOnline) };
		    false ->
			State0
	     end
	end,
    {noreply, 
     case lists:keytake(Pid, 2, UpstOnline) of
	 {value, {PRef, Pid}, UpstOnline1} -> 
	     {ok, PState1} = PMod:handle_connect({down, Reason}, upstream, PRef, PState),
	     RemoveDownst(
	       State#state{pstate       = PState1, 
			   upst_online  = UpstOnline1, 
			   upst_offline = [PRef | UpstOffline]});
	 false ->
	     RemoveDownst(State)
     end}.


terminate(Reason, #state{pstate = PState,
			 mod    = PMod} ) ->
    PMod:terminate(Reason, PState),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
