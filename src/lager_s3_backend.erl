%%%-------------------------------------------------------------------
%%% @author Twinny130
%%% @copyright (C) 2017, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 07. 2월 2017 오후 5:46
%%%-------------------------------------------------------------------
-module(lager_s3_backend).
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
  s3_bucket :: list(),
  log_file :: list(),
  aws_config :: #aws_config{},
  log :: binary() }).
-compile({parse_transform, lager_transform}).

init(Config) when is_list(Config) ->
  application:ensure_all_started(lhttpc),

  Level = proplists:get_value(level, Config),
  Formatter = proplists:get_value(formatter, Config, lager_default_formatter),
  FormatterConfig = proplists:get_value(formatter_config, Config),
  Bucket = proplists:get_value(s3_bucket, Config),
  LogFile = proplists:get_value(log_file, Config),
  AWSConfig = get_aws_config(proplists:get_value(aws_config, Config, undefined)),
  Log = load_log_file_from_s3(Bucket, LogFile, AWSConfig),

  {ok, #state{
    level=lager_util:config_to_mask(Level),
    formatter = Formatter,
    format_config = FormatterConfig,
    s3_bucket = Bucket,
    log_file = LogFile,
    aws_config = AWSConfig,
    log = Log
  }}.

handle_call(get_loglevel, #state{level=Level} = State) ->
  {ok, Level, State};

handle_call({set_loglevel, Level}, State) ->
  {ok, ok, State#state{level=lager_util:config_to_mask(Level)}};

handle_call({set_aws_config, Config}, State) ->
  {ok, ok, State#state{aws_config = get_aws_config(Config)}};

handle_call(load_log_file, State) ->
  try load_log_file_from_s3(State#state.s3_bucket, State#state.log_file, State#state.aws_config) of
    Log -> {ok, ok, State#state{log = Log}}
  catch
    _:_ -> {ok, {error, fail_to_load}, State}
  end;

handle_call(_Request, State) ->
  {ok, ok, State}.

handle_event({log, Message}, State) ->
  #state{
    level = Level,
    formatter=Formatter,
    format_config=FormatConfig,
    s3_bucket = Bucket,
    log_file = LogFile,
    aws_config = AWSConfig,
    log = Log
  } = State,

  case lager_util:is_loggable(Message, Level, ?MODULE) of
    true ->
      Msg = lists:flatten( Formatter:format(Message,FormatConfig) ),
      MsgBin = list_to_binary(Msg),
      NewLog = <<Log/binary, MsgBin/binary>>,
      upload_log_to_s3(Bucket, LogFile, AWSConfig, NewLog),
      {ok, State#state{log = NewLog}};
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
%% internal function
%%

load_log_file_from_s3(Bucket, Key, Config) ->
  File =
    try erlcloud_s3:get_object(Bucket, Key, Config)
    catch
      _:{aws_error,{http_error,404, _, _}} -> [{content, <<"">>}];
      _:Error -> throw(Error)
    end,
  proplists:get_value(content, File).

upload_log_to_s3(Bucket, Key, Config, Log) ->
  {ok, Attrs} = erlcloud_s3:start_multipart(Bucket, Key, [], [], Config),
  UploadId = proplists:get_value(uploadId, Attrs),
  upload_parts_to_s3(Bucket, Key, Config, UploadId, 1, Log, []).

upload_parts_to_s3(Bucket, Key, Config, UploadId, _Num, <<>>, Etags) ->
  erlcloud_s3:complete_multipart(Bucket, Key, UploadId, Etags, [], Config),
  ok;

upload_parts_to_s3(Bucket, Key, Config, UploadId, Num, Log, Etags) ->
  MaxBytes = 6000000,
  LogSize = byte_size(Log),
  PartSize = erlang:min(MaxBytes, LogSize),
  {Part, Remain} = split_bytes(PartSize, Log),

  {ok, PartAttrs} = erlcloud_s3:upload_part(Bucket, Key, UploadId, Num, Part, [], Config),
  NextEtags = Etags ++ [{Num, proplists:get_value(etag, PartAttrs)}],
  upload_parts_to_s3(Bucket, Key, Config, UploadId, Num+1, Remain, NextEtags).


split_bytes(Bytes, Content) ->
  <<LeadingBytes:Bytes/binary-unit:8, T/binary>> = Content,
  {LeadingBytes, T}.

get_aws_config(profile) ->
  {ok, Config} = erlcloud_aws:profile(),
  Config;

get_aws_config(Config) when is_list(Config) ->
  #aws_config{
    s3_host = proplists:get_value(s3_host, Config),
    access_key_id = proplists:get_value(access_key_id, Config),
    secret_access_key = proplists:get_value(secret_access_key, Config)};

get_aws_config(#aws_config{} = Config) -> Config;

get_aws_config(_) -> #aws_config{}.