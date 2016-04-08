-module(lager_manager_killer).
-author("Sungjin Park <jinni.park@gmail.com>").
-behavior(gen_event).

-export([init/1, handle_event/2, handle_call/2, handle_info/2, terminate/2, code_change/3]).

-include("lager.hrl").

-record(state, {
          killer_hwm             :: pos_integer(),
          killer_reinstall_after :: pos_integer(),
          killer_tick            :: pos_integer() | test,
          mailbox_queue_len = 0  :: non_neg_integer()
         }).

init([KillerHWM, KillerReinstallAfter, test]) ->
    {ok, #state{killer_hwm=KillerHWM, killer_reinstall_after=KillerReinstallAfter, killer_tick=test}};
init([KillerHWM, KillerReinstallAfter, KillerTick]) ->
    erlang:send_after(KillerTick, self(), killer_tick),
    {ok, #state{killer_hwm=KillerHWM, killer_reinstall_after=KillerReinstallAfter, killer_tick=KillerTick}}.

handle_call(get_loglevel, State) ->
    {ok, {mask, ?LOG_NONE}, State};
handle_call({set_loglevel, _Level}, State) ->
    {ok, ok, State};
handle_call(get_settings, State = #state{killer_hwm=KillerHWM, killer_reinstall_after=KillerReinstallAfter, killer_tick=KillerTick}) ->
    {ok, [KillerHWM, KillerReinstallAfter, KillerTick], State};
handle_call(_Request, State) ->
    {ok, ok, State}.

%% Checking the mailbox length for EVERY SINGLE log message is probably not a
%% good idea but for testing purposes, it is great and reduces timing
%% non-determinism.
handle_event({log, _Message}, State = #state{killer_hwm=HWM, killer_reinstall_after=A, killer_tick=test}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    case Len > HWM of
        true ->
            exit({kill_me, [HWM, A, test]});
        _ ->
            {ok, State}
    end;
handle_event({log, _Message}, State) ->
    {ok, State};
handle_event(_Event, State) ->
    {ok, State}.

handle_info(killer_tick, State=#state{killer_hwm = HWM, killer_reinstall_after=A, 
                                      killer_tick=T, mailbox_queue_len = QL}) when QL > HWM ->
    exit({kill_me, [HWM, A, T]}),
    {ok, State};
handle_info(killer_tick, State=#state{killer_tick = T}) ->
    {message_queue_len, Len} = process_info(self(), message_queue_len),
    erlang:send_after(T, self(), killer_tick),
    {ok, State#state{mailbox_queue_len = Len}};
handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
