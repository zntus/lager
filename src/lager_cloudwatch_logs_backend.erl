%%%-------------------------------------------------------------------
%%% @author Twinny130
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 10. 2월 2017 오후 9:11
%%%-------------------------------------------------------------------
-module(lager_cloudwatch_logs_backend).
-author("Twinny130").
-include("lager.hrl").
-include_lib("erlcloud/include/erlcloud.hrl").
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2, terminate/2,
  code_change/3]).

-record(state, {
  level :: {'mask', integer()},
  formatter :: atom(),
  format_config :: any(),
  log_group :: string(),
  log_stream :: string() | atom(),
  sequence_token :: string() | binary(),
  aws_config :: #aws_config{} }).
-compile({parse_transform, lager_transform}).

init(Config) when is_list(Config) ->
  application:ensure_all_started(lhttpc),

  Level = proplists:get_value(level, Config),
  Formatter = proplists:get_value(formatter, Config, lager_default_formatter),
  FormatterConfig = proplists:get_value(formatter_config, Config),
  LogGroupName = proplists:get_value(log_group, Config),
  LogStreamName = proplists:get_value(log_stream, Config),
  AWSConfig = get_aws_config(proplists:get_value(aws_config, Config, undefined)),
  ok = create_log_stream_if_no_exists(LogGroupName, LogStreamName, AWSConfig),
  SeqToken = get_sequence_token(LogGroupName, LogStreamName, AWSConfig),

  {ok, #state{
    level=lager_util:config_to_mask(Level),
    formatter = Formatter,
    format_config = FormatterConfig,
    log_group = LogGroupName,
    log_stream = LogStreamName,
    sequence_token = SeqToken,
    aws_config = AWSConfig
  }}.

handle_call(get_loglevel, #state{level=Level} = State) ->
  {ok, Level, State};

handle_call({set_loglevel, Level}, State) ->
  {ok, ok, State#state{level=lager_util:config_to_mask(Level)}};

handle_call({set_aws_config, Config}, State) ->
  {ok, ok, State#state{aws_config = get_aws_config(Config)}};

handle_call(_Request, State) ->
  {ok, ok, State}.

handle_event({log, Message}, State) ->
  #state{
    level = Level,
    formatter=Formatter,
    format_config=FormatConfig,
    log_group = LogGroupName,
    log_stream = LogStreamName,
    sequence_token = SeqToken,
    aws_config = AWSConfig
  } = State,

  case lager_util:is_loggable(Message, Level, ?MODULE) of
    true ->
      Msg = lists:flatten( Formatter:format(Message,FormatConfig) ),
      MsgBin = list_to_binary(Msg),
      NextSeqToken = push_log_to_cloudwatch_logs(LogGroupName, LogStreamName, SeqToken, MsgBin, AWSConfig),
      {ok, State#state{sequence_token = NextSeqToken}};
    false ->
      {ok, State}
  end;

handle_event(_Event, State) ->
  {ok, State}.

handle_info(_Info, State) ->
  {ok, State}.

terminate(_Reason, _State) ->
  ok.

code_change(_OldVsn, State, _Extra) ->
  {ok, State}.

%%
%% internal_function
%%

create_log_stream(LogGroupName, LogStreamName, Config) ->
  try erlcloud_cloudwatch_logs:create_log_stream(LogGroupName, LogStreamName, Config) catch
    _:Err -> throw(Err)
  end.

create_log_stream_if_no_exists(LogGroupName, LogStreamName, Config) ->
  {ok, LogStreams, _} = erlcloud_cloudwatch_logs:describe_log_streams(LogGroupName, LogStreamName, Config),

  Exists =
    lists:filter(
      fun(Stream) ->
        case
          lists:keyfind(<<"logStreamName">>, 1, Stream)
        of
          {<<"logStreamName">>, _} -> true;
          _ -> false
        end
      end,
      LogStreams),

  case Exists of
    [_Stream | _] -> ok;
    _Else -> create_log_stream(LogGroupName, LogStreamName, Config)
  end.

get_sequence_token(LogGroupName, LogStreamName, Config) ->
  {ok, LogStreams, _} = erlcloud_cloudwatch_logs:describe_log_streams(LogGroupName,LogStreamName, Config),

  [LogStream | _] =
    lists:filter(
      fun(Stream) ->
        case
          lists:keyfind(<<"logStreamName">>, 1, Stream)
        of
          {<<"logStreamName">>, _} -> true;
          _ -> false
        end
      end,
      LogStreams),

  proplists:get_value(<<"uploadSequenceToken">>, LogStream, undefined).

push_log_to_cloudwatch_logs(LogGroupName, LogStreamName, SeqToken, Msg, Config) ->
  LogEvents = [
    [ {<<"message">>, Msg},
      {<<"timestamp">>, get_timestamp()}]
  ],
  Result = erlcloud_cloudwatch_logs:put_log_events(LogEvents, LogGroupName, LogStreamName, SeqToken, Config),
  {ok, NextSequenceToken, _RejectedLogEventsInfo} = Result,
  NextSequenceToken.

get_aws_config(profile) ->
  {ok, Config} = erlcloud_aws:profile(),
  Config;

get_aws_config(Config) when is_list(Config) ->
  #aws_config{
    cloudwatch_logs_host = proplists:get_value(cloudwatch_logs_host, Config),
    access_key_id = proplists:get_value(access_key_id, Config),
    secret_access_key = proplists:get_value(secret_access_key, Config)};

get_aws_config(#aws_config{} = Config) -> Config;

get_aws_config(_) -> #aws_config{}.

get_timestamp() ->
  {Mega, Sec, Micro} = os:timestamp(),
  (Mega*1000000 + Sec)*1000 + round(Micro/1000).