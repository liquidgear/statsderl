-module(statsderl).
-include("statsderl.hrl").

-compile(inline).
-compile({inline_size, 512}).

%% public
-export([
    counter/3, counter/4,
    decrement/3, decrement/4,
    gauge/3, gauge/4,
    gauge_decrement/3, gauge_decrement/4,
    gauge_increment/3, gauge_increment/4,
    increment/3, increment/4,
    timing/3, timing/4, timing_fun/3, timing_fun/4,
    timing_now/3, timing_now/4,
    timing_now_us/3, timing_now_us/4
]).

%% public
-spec counter(key(), value(), sample_rate()) ->
    ok.

counter(Key, Value, Rate) ->
    statsderl_pool:sample(Rate, {counter, Key, Value, Rate}).

-spec counter(key(), value(), sample_rate(), influx_tags()) ->
    ok.

counter(Key, Value, Rate, AddTags) when is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    counter(ActualKey, Value, Rate).

-spec decrement(key(), value(), sample_rate()) ->
    ok.

decrement(Key, Value, Rate) when Value >= 0 ->
    statsderl_pool:sample(Rate, {counter, Key, -Value, Rate}).

-spec decrement(key(), value(), sample_rate(), influx_tags()) ->
    ok.

decrement(Key, Value, Rate, AddTags) when Value >= 0, is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {counter, ActualKey, -Value, Rate}).

-spec gauge(key(), value(), sample_rate()) ->
    ok.

gauge(Key, Value, Rate) when Value >= 0 ->
    statsderl_pool:sample(Rate, {gauge, Key, Value}).

-spec gauge(key(), value(), sample_rate(), influx_tags()) ->
    ok.

gauge(Key, Value, Rate, AddTags) when Value >= 0, is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {gauge, ActualKey, Value}).

-spec gauge_decrement(key(), value(), sample_rate()) ->
    ok.

gauge_decrement(Key, Value, Rate) when Value >= 0 ->
    statsderl_pool:sample(Rate, {gauge_decrement, Key, Value}).

-spec gauge_decrement(key(), value(), sample_rate(), influx_tags()) ->
    ok.

gauge_decrement(Key, Value, Rate, AddTags) when Value >= 0, is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {gauge_decrement, ActualKey, Value}).

-spec gauge_increment(key(), value(), sample_rate()) ->
    ok.

gauge_increment(Key, Value, Rate) when Value >= 0 ->
    statsderl_pool:sample(Rate, {gauge_increment, Key, Value}).

-spec gauge_increment(key(), value(), sample_rate(), influx_tags()) ->
    ok.

gauge_increment(Key, Value, Rate, AddTags) when Value >= 0, is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {gauge_increment, ActualKey, Value}).

-spec increment(key(), value(), sample_rate()) ->
    ok.

increment(Key, Value, Rate) when Value >= 0 ->
    statsderl_pool:sample(Rate, {counter, Key, Value, Rate}).

-spec increment(key(), value(), sample_rate(), influx_tags()) ->
    ok.

increment(Key, Value, Rate, AddTags) when Value >= 0, is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {counter, ActualKey, Value, Rate}).

-spec timing(key(), value(), sample_rate()) ->
    ok.

timing(Key, Value, Rate) ->
    statsderl_pool:sample(Rate, {timing, Key, Value}).

-spec timing(key(), value(), sample_rate(), influx_tags()) ->
    ok.

timing(Key, Value, Rate, AddTags) when is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {timing, ActualKey, Value}).

-spec timing_fun(key(), fun(), sample_rate()) ->
    ok.

timing_fun(Key, Fun, Rate) ->
    Timestamp = statsderl_utils:timestamp(),
    Result = Fun(),
    timing_now(Key, Timestamp, Rate),
    Result.

-spec timing_fun(key(), fun(), sample_rate(), influx_tags()) ->
    ok.

timing_fun(Key, Fun, Rate, AddTags) when is_map(AddTags) ->
    Timestamp = statsderl_utils:timestamp(),
    Result = Fun(),
    ActualKey = format_key(Key, AddTags),
    timing_now(ActualKey, Timestamp, Rate),
    Result.

-spec timing_now(key(), erlang:timestamp(), sample_rate()) ->
    ok.

timing_now(Key, Timestamp, Rate) ->
    statsderl_pool:sample(Rate, {timing_now, Key, Timestamp}).

-spec timing_now(key(), erlang:timestamp(), sample_rate(), influx_tags()) ->
    ok.

timing_now(Key, Timestamp, Rate, AddTags) when is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {timing_now, ActualKey, Timestamp}).

-spec timing_now_us(key(), erlang:timestamp(), sample_rate()) ->
    ok.

timing_now_us(Key, Timestamp, Rate) ->
    statsderl_pool:sample(Rate, {timing_now_us, Key, Timestamp}).

-spec timing_now_us(key(), erlang:timestamp(), sample_rate(), influx_tags()) ->
    ok.

timing_now_us(Key, Timestamp, Rate, AddTags) when is_map(AddTags) ->
    ActualKey = format_key(Key, AddTags),
    statsderl_pool:sample(Rate, {timing_now_us, ActualKey, Timestamp}).


%% private
format_key(BaseKey, AddTags) ->
    BaseTags =
        case erlang:get(?ENV_INFLUX_BASE_TAGS) of
            undefined ->
                Value = discover_standard_tags(?ENV(?ENV_INFLUX_BASE_TAGS, #{})),
                erlang:put(?ENV_INFLUX_BASE_TAGS, Value),
                Value;
            Value ->
                Value
        end,
    FormattedTags =
        maps:fold(
            fun(TagKey, TagValue, Acc) ->
                [<<",">>, to_iodata(TagKey), <<"=">>, to_iodata(TagValue) | Acc]
            end, [], maps:merge(BaseTags, AddTags)
        ),
    [BaseKey, FormattedTags].

to_iodata(What) when is_binary(What) ->
    What;
to_iodata(What) when is_list(What) ->
    What;
to_iodata(What) when is_atom(What) ->
    atom_to_binary(What, utf8);
to_iodata(What) when is_integer(What) ->
    integer_to_binary(What);
to_iodata(What) when is_float(What) ->
    float_to_binary(What).

discover_standard_tags(BaseTags) ->
    discover_standard_tags(BaseTags, [node]).

discover_standard_tags(BaseTags, []) ->
    BaseTags;

discover_standard_tags(BaseTags, [node | Rest]) ->
    Tmp =
        case erlang:node() of
            'nonode@nohost' ->
                BaseTags;
            Value ->
                BaseTags#{ node => Value}
        end,
    discover_standard_tags(Tmp, Rest).
