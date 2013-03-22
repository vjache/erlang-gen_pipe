%%%-------------------------------------------------------------------
%%% @author Vyacheslav Vorobyov <vjache@gmail.com>
%%% @copyright (C) 2013, Vyacheslav Vorobyov
%%% @doc
%%%
%%% @end
%%%-------------------------------------------------------------------

-module(udp_pipe).

-behavior(gen_pipe).

% Export 'gen_pipe' callbacks.
-export([init/1,
	 terminate/2,
	 handle_connect/4,
	 handle_data/3]).

-export([start_link/3, start_link/2]).

-define(ECHO(Msg), io:format("(~p|~p|~p) : ~p~n", [?MODULE, self(), ?LINE, Msg])).

-record(state, {local_end, remote_end, sock}).

start_link(PipeName, LocalEndpoint) ->
    start_link(PipeName, 
	       LocalEndpoint, 
	       undefined).

start_link(PipeName, 
	   LocalEndpoint, 
	   RemoteEndpoint) ->
    case LocalEndpoint of
	_ when is_integer(LocalEndpoint) -> 
	    LocalEndpoint1 = { {127,0,0,1}, LocalEndpoint}; 
	{ {_,_,_,_}, _} ->  
	    LocalEndpoint1 = LocalEndpoint
    end,
    gen_pipe:start_link(
      PipeName, ?MODULE, {LocalEndpoint1, RemoteEndpoint}, [], []).

init({ {IPAddress, UdpPort} = LocalEndpoint, RemoteEndpoint}) ->
    ?ECHO({init, LocalEndpoint, RemoteEndpoint}),
    {ok, Sock} = gen_udp:open(UdpPort, [{ip, IPAddress}, binary]),
    {ok, #state{local_end = LocalEndpoint, 
		remote_end = RemoteEndpoint, 
		sock = Sock}}.

terminate(_Reason, #state{sock = Sock} = State) ->
    ok = gen_udp:close(Sock),
    {ok, State}.

handle_connect(_Status, _Type, _PipeRef, State) ->
    ?ECHO({handle_connect, _Status, _Type, _PipeRef, State}),
    {ok, State}.

handle_data({udp, Sock, RAddr, RPort, PacketFromNet} = Msg, 
	    info, #state{sock = Sock, remote_end = R} = State) ->
    ?ECHO({handle_data_info, Msg}),
    % Validate packet source
    case R of
	undefined      -> ok; % we do not want to restrict source
	{RAddr, RPort} -> ok;
	BadSourceAddr  -> exit({bad_source, BadSourceAddr})
    end,
    { {out, PacketFromNet}, State };
handle_data(_Info, 
	    info, State) ->
    ?ECHO({unexpected_info, _Info}),
    { nout, State };
handle_data(PacketToNet, _UpstPipeRef, 
	    #state{sock = Sock, remote_end = {RAddr, RPort}} = State) ->
    ok = gen_udp:send(Sock, RAddr, RPort, PacketToNet),
    {nout, State}.

    


