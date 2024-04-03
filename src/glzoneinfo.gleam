//// Parse the Time Zone Information Format (TZif) described in [RFC 8536](https://datatracker.ietf.org/doc/html/rfc8536)
//// At the moment it supports versions 1 and 2. Version 3 files will be parsed, but include only information up to V2.

import gleam/option
import gleam/bit_array
import gleam/int
import gleam/list
import gleam/result
import gleam/string

pub type Zone {
  Zone(
    version: Version,
    transitions: List(TransitionInfo),
    leaps: List(LeapSecond),
    rule: option.Option(Rule),
  )
}

pub type Version {
  V1
  V2
  V3
}

pub type TransitionInfo {
  TransitionInfo(
    gmt_offset: Int,
    starts_at: Int,
    is_dst: Bool,
    abbrev_index: Int,
    abbreviation: String,
    is_std: Bool,
    is_utc: Bool,
  )
}

pub type LeapSecond {
  LeapSecond(epoch: Int, correction: Int)
}

pub type Rule {
  Rule(
    std_abbr: option.Option(String),
    std_offset: Int,
    dst_abbr: option.Option(String),
    dst_offset: Int,
    start_time: option.Option(Date),
    end_time: option.Option(Date),
  )
}

pub type Time {
  Time(hours: Int, minutes: Int, seconds: Int)
}

pub type Date {
  Date(month: Int, week: Int, day: Int)
}

pub type Header {
  Header(
    utc_count: Int,
    wall_count: Int,
    leap_count: Int,
    transition_count: Int,
    type_count: Int,
    abbrev_length: Int,
  )
}

pub type Errors {
  InvalidZoneName
  ParseError(String, BitArray)
}

pub fn parse(binary: BitArray) {
  let zone = Zone(version: V1, transitions: [], leaps: [], rule: option.None)

  case binary {
    <<"TZif1":utf8, _reserved:bytes-size(15), rest:bytes>> -> {
      use #(zone, _) <- result.try(parse_v1(zone, rest))
      Ok(zone)
    }
    <<"TZif2":utf8, _reserved:bytes-size(15), rest:bytes>> -> {
      use #(zone, rest1) <- result.try(parse_v1(zone, rest))
      use #(zone, _rest2) <- result.try(parse_v2(zone, rest1))
      Ok(zone)
    }
    <<"TZif3":utf8, _reserved:bytes-size(15), rest:bytes>> -> {
      use #(zone, rest1) <- result.try(parse_v1(zone, rest))
      use #(zone, _rest2) <- result.try(parse_v3(zone, rest1))
      Ok(zone)
    }
    _ -> Error(ParseError("parse", binary))
  }
}

fn parse_v1(zone: Zone, binary: BitArray) {
  case binary {
    <<header_raw:bytes-size(24), rest:bytes>> -> {
      use header <- result.try(parse_header(header_raw))
      parse_transition_times(header, zone, rest)
    }
    _ -> Error(ParseError("parse_content", binary))
  }
}

fn parse_v2(zone_v1: Zone, binary: BitArray) {
  case binary {
    <<"TZif2":utf8, _reserved:bytes-size(15), rest:bytes>> -> {
      use #(zone_v2, _) <- result.try(parse_v1(
        Zone(..zone_v1, version: V2),
        rest,
      ))

      Ok(#(
        Zone(
          version: V2,
          transitions: list.concat([zone_v1.transitions, zone_v2.transitions])
            |> list.sort(fn(a: TransitionInfo, b: TransitionInfo) {
              int.compare(a.starts_at, b.starts_at)
            }),
          leaps: list.concat([zone_v1.leaps, zone_v2.leaps])
            |> list.sort(fn(a: LeapSecond, b: LeapSecond) {
              int.compare(a.epoch, b.epoch)
            }),
          rule: zone_v2.rule,
        ),
        rest,
      ))
    }
    _ -> Error(ParseError("parse_v2", binary))
  }
}

fn parse_v3(zone_v1: Zone, binary: BitArray) {
  case binary {
    <<"TZif3":utf8, _reserved:bytes-size(15), rest:bytes>> -> {
      use #(zone_v3, _) <- result.try(parse_v1(
        Zone(..zone_v1, version: V3),
        rest,
      ))

      Ok(#(
        Zone(
          version: V2,
          transitions: list.concat([zone_v1.transitions, zone_v3.transitions])
            |> list.sort(fn(a: TransitionInfo, b: TransitionInfo) {
              int.compare(a.starts_at, b.starts_at)
            }),
          leaps: list.concat([zone_v1.leaps, zone_v3.leaps])
            |> list.sort(fn(a: LeapSecond, b: LeapSecond) {
              int.compare(a.epoch, b.epoch)
            }),
          rule: zone_v3.rule,
        ),
        rest,
      ))
    }
    _ -> Error(ParseError("parse_v3", binary))
  }
}

fn parse_transition_times(header: Header, zone: Zone, binary: BitArray) {
  use #(times, rest) <- result.try(parse_array(
    binary,
    header.transition_count,
    int_parser(zone.version),
  ))

  parse_transition_info(header, rest, zone, times)
}

fn parse_transition_info(
  header: Header,
  binary: BitArray,
  zone: Zone,
  times: List(Int),
) {
  use #(indices, rest) <- result.try(parse_array(
    binary,
    header.transition_count,
    uchar_parser,
  ))

  use #(txinfos, rest) <- result.try(parse_array(
    rest,
    header.type_count,
    txinfo_parser,
  ))

  let txs =
    indices
    |> list.map(list.at(txinfos, _))
    |> result.values()
    |> list.zip(times)
    |> list.map(fn(t) { TransitionInfo(..t.0, starts_at: t.1) })

  parse_abbreviations(rest, header, Zone(..zone, transitions: txs))
}

fn parse_abbreviations(binary: BitArray, header: Header, zone: Zone) {
  let len = header.abbrev_length

  use #(abbrevs, rest) <- result.try(case binary {
    <<abbrevs:bytes-size(len), rest:bytes>> -> Ok(#(abbrevs, rest))
    _ -> Error(ParseError("parse_abbreviations", binary))
  })

  let updated_transitions =
    zone.transitions
    |> list.map(fn(transition) {
      use part <- result.try(binary_part(
        abbrevs,
        transition.abbrev_index,
        len - transition.abbrev_index,
      ))

      use #(abbrev, _) <- result.try(parse_null_terminated_string(part))

      Ok(TransitionInfo(..transition, abbreviation: abbrev))
    })
    |> result.values()

  parse_leap_seconds(
    rest,
    header,
    Zone(..zone, transitions: updated_transitions),
  )
}

fn parse_leap_seconds(binary: BitArray, header: Header, zone: Zone) {
  use #(leaps, rest) <- result.try(
    parse_array(binary, header.leap_count, fn(bin) {
      use #(epoch, next) <- result.try(int_parser(zone.version)(bin))
      use #(correction, next) <- result.try(int32_parser(next))
      Ok(#(LeapSecond(epoch: epoch, correction: correction), next))
    }),
  )

  parse_flags(rest, header, Zone(..zone, leaps: leaps))
}

fn parse_flags(binary: BitArray, header: Header, zone: Zone) {
  use #(is_std_indicators, rest) <- result.try(parse_array(
    binary,
    header.wall_count,
    char_parser,
  ))

  use #(is_utc_indicators, rest) <- result.try(parse_array(
    rest,
    header.utc_count,
    char_parser,
  ))

  let updated_transitions =
    zone.transitions
    |> list.index_map(fn(transition, index) {
      TransitionInfo(
        ..transition,
        is_std: is_std_indicators
        |> list.at(index)
        |> result.unwrap(0)
        == 1,
        is_utc: is_utc_indicators
        |> list.at(index)
        |> result.unwrap(0)
        == 1,
      )
    })

  let new_zone = Zone(..zone, transitions: updated_transitions)

  case zone.version {
    V1 -> Ok(#(new_zone, rest))
    V2 | V3 -> parse_posixtz_string(rest, new_zone)
  }
}

fn parse_posixtz_string(binary: BitArray, zone: Zone) {
  use #(format_string, rest) <- result.try(parse_newline_terminated_string(
    binary,
  ))
  use #(rule, format_rest) <- result.try(parse_tz(format_string))

  Ok(#(Zone(..zone, rule: rule), <<rest:bits, format_rest:bits>>))
}

fn parse_tz(binary: BitArray) {
  case binary {
    <<>> -> Ok(#(option.None, <<>>))
    _ -> parse_tz_std_abbr(binary)
  }
}

fn parse_tz_std_abbr(binary: BitArray) {
  use #(abbr_bits, rest) <- result.try(parse_abbrev(binary))
  let abbr = bit_array.to_string(abbr_bits)

  parse_tz_std_offset(
    rest,
    Rule(
      std_abbr: option.from_result(abbr),
      std_offset: 0,
      dst_abbr: option.from_result(abbr),
      dst_offset: 0,
      start_time: option.None,
      end_time: option.None,
    ),
  )
}

fn parse_tz_std_offset(binary: BitArray, rule: Rule) {
  case parse_offset(binary) {
    Ok(#(offset, <<>> as r)) ->
      Ok(#(option.Some(Rule(..rule, std_offset: offset, dst_offset: offset)), r))
    Ok(#(offset, rest)) ->
      rest
      |> parse_tz_dst_abbr(Rule(..rule, std_offset: offset, dst_offset: offset))
    Error(ParseError(_, <<>> as r)) -> Ok(#(option.Some(rule), r))
    Error(ParseError("parsing hours failed", rest)) ->
      parse_tz_dst_abbr(rest, rule)
    Error(err) -> Error(err)
  }
}

fn parse_tz_dst_abbr(binary: BitArray, rule: Rule) {
  case parse_abbrev(binary) {
    Ok(#(abbr, rest)) -> {
      let rule =
        Rule(..rule, dst_abbr: option.from_result(bit_array.to_string(abbr)))

      case rest {
        <<>> -> Ok(#(option.Some(rule), <<>>))
        <<",":utf8, rest:bytes>> -> parse_tz_rule_period(rest, rule)
        _ -> parse_tz_dst_offset(rest, rule)
      }
    }
    _ -> parse_tz_dst_offset(binary, rule)
  }
}

fn parse_tz_dst_offset(binary: BitArray, rule: Rule) {
  case parse_offset(binary) {
    Ok(#(offset, rest)) -> {
      let rule = Rule(..rule, dst_offset: offset)

      case rest {
        <<>> -> Ok(#(option.Some(rule), <<>>))
        <<",":utf8, rest:bytes>> -> parse_tz_rule_period(rest, rule)
        _ -> Error(ParseError("invalid tz rule format", rest))
      }
    }
    Error(ParseError(_, <<",":utf8, rest:bytes>>)) ->
      parse_tz_rule_period(rest, rule)
    Error(err) -> Error(err)
  }
}

fn parse_tz_rule_period(binary: BitArray, rule: Rule) {
  use #(start_dt, end_dt) <- result.try(split(binary, <<",":utf8>>))
  use #(start_time, _) <- result.try(parse_posixtz_datetime(start_dt))
  use #(end_time, rest) <- result.try(parse_posixtz_datetime(end_dt))
  Ok(#(
    option.Some(
      Rule(
        ..rule,
        start_time: option.Some(start_time),
        end_time: option.Some(end_time),
      ),
    ),
    rest,
  ))
}

fn parse_posixtz_datetime(binary: BitArray) {
  case binary {
    <<"M":utf8, rest:bytes>> -> parse_month_week_day(rest)
    _ -> Error(ParseError("Unsupported POSIX datetime", binary))
  }
}

fn parse_month_week_day(binary: BitArray) {
  use #(month_raw, rest) <- result.try(split(binary, <<".":utf8>>))
  use #(week_raw, day_raw) <- result.try(split(rest, <<".":utf8>>))
  use #(month, _) <- result.try(parse_uint(month_raw))
  use #(week, _) <- result.try(parse_uint(week_raw))
  use #(day, _) <- result.try(parse_uint(day_raw))
  Ok(#(Date(month, week, day), <<>>))
}

fn split(
  binary: BitArray,
  sep: BitArray,
) -> Result(#(BitArray, BitArray), Errors) {
  split_rec(binary, sep, <<>>)
}

fn split_rec(binary, sep, acc) -> Result(#(BitArray, BitArray), Errors) {
  case binary {
    <<c:bytes-size(1), rest:bytes>> if c == sep -> Ok(#(acc, rest))
    <<c:bytes-size(1), rest:bytes>> ->
      split_rec(rest, sep, <<acc:bits, c:bits>>)
    _ -> Error(ParseError("couldn't split BitArray, expected comma", binary))
  }
}

fn parse_offset(binary: BitArray) {
  let #(sign, rest) = case binary {
    <<"+":utf8, rest:bytes>> -> #(1, rest)
    <<"-":utf8, rest:bytes>> -> #(-1, rest)
    _ -> #(1, binary)
  }

  use #(time, rest) <- result.try(parse_time(rest))

  let total = {
    { time.hours * 60 * 24 } + { time.minutes * 60 } + time.seconds
  }

  Ok(#(sign * total, rest))
}

fn parse_time(binary: BitArray) {
  case parse_uint(binary) {
    Ok(#(hh, <<":":utf8, rest:bytes>>)) if hh >= 0 && hh <= 24 ->
      case parse_uint(rest) {
        Ok(#(mm, <<":":utf8, rest:bytes>>)) if mm >= 0 && mm <= 60 ->
          case parse_uint(rest) {
            Ok(#(ss, rest)) if ss >= 0 && ss <= 60 ->
              Ok(#(Time(hh, mm, ss), rest))
            _ -> Error(ParseError("parsing seconds failed", rest))
          }
        Ok(#(mm, rest)) if mm >= 0 && mm <= 60 -> Ok(#(Time(hh, mm, 0), rest))
        _ -> Error(ParseError("parsing minutes failed", rest))
      }

    Ok(#(hh, rest)) if hh >= 0 && hh <= 24 -> Ok(#(Time(hh, 0, 0), rest))
    _ -> Error(ParseError("parsing hours failed", binary))
  }
}

fn parse_uint(binary: BitArray) {
  parse_uint_rec(binary, 0, 0)
}

fn parse_uint_rec(binary, iteration, acc) -> Result(#(Int, BitArray), Errors) {
  case binary {
    <<n:8, rest:bytes>> if n >= 48 && n <= 57 ->
      parse_uint_rec(rest, iteration + 1, acc * 10 + n - 48)
    _ if iteration > 0 -> Ok(#(acc, binary))
    _ -> Error(ParseError("invalid integer format", binary))
  }
}

fn parse_abbrev(binary: BitArray) {
  case binary {
    <<"<":utf8, rest:bytes>> -> parse_quoted_abbrev(rest, <<>>)
    _ -> parse_unquoted_abbrev(binary, <<>>)
  }
}

fn parse_quoted_abbrev(binary: BitArray, result) {
  let len = bit_array.byte_size(result)
  case binary {
    <<">":utf8, _rest:bytes>> if len < 3 ->
      Error(ParseError("quoted abbreviation too short", binary))
    <<">":utf8, rest:bytes>> -> Ok(#(result, rest))
    <<char:int-size(1)-unit(8), rest:bytes>> ->
      parse_quoted_abbrev(rest, <<result:bits, char>>)
    <<>> -> Error(ParseError("quoted abbreviation unclosed", binary))
    _ -> Error(ParseError("quoted abbreviation invalid", binary))
  }
}

fn parse_unquoted_abbrev(binary: BitArray, result) {
  let len = bit_array.byte_size(result)
  case binary {
    <<char:int-size(1)-unit(8), rest:bytes>>
      if char >= 65 && char <= 90 || char >= 97 && char <= 122
    -> parse_unquoted_abbrev(rest, <<result:bits, char>>)
    _ if len < 3 -> Error(ParseError("unquoted abbreviation too short", binary))
    _ if len > 6 -> Error(ParseError("unquoted abbreviation invalid", binary))
    _ -> Ok(#(result, binary))
  }
}

fn binary_part(binary, pos, len) {
  case binary {
    <<_:bytes-size(pos), val:bytes-size(len), _:bytes>> -> Ok(val)
    _ -> Error(ParseError("binary_part", binary))
  }
}

fn parse_null_terminated_string(binary: BitArray) {
  parse_null_terminated_string_rec(binary, [])
}

fn parse_null_terminated_string_rec(
  binary: BitArray,
  result: List(UtfCodepoint),
) {
  case binary {
    <<>> -> Ok(#(string.from_utf_codepoints(list.reverse(result)), binary))
    <<0, rest:bytes>> ->
      Ok(#(string.from_utf_codepoints(list.reverse(result)), rest))
    <<char:int-size(1)-unit(8), rest:bytes>> ->
      case string.utf_codepoint(char) {
        Ok(codepoint) ->
          parse_null_terminated_string_rec(rest, [codepoint, ..result])
        Error(Nil) ->
          Error(ParseError("parse_null_terminated_string_0", binary))
      }
    _ -> Error(ParseError("parse_null_terminated_string_1", binary))
  }
}

fn parse_newline_terminated_string(
  binary: BitArray,
) -> Result(#(BitArray, BitArray), Errors) {
  use #(str, rest) <- result.try(
    parse_newline_terminated_string_start(binary, []),
  )
  Ok(#(bit_array.concat(str), rest))
}

fn parse_newline_terminated_string_start(
  binary: BitArray,
  result: List(BitArray),
) -> Result(#(List(BitArray), BitArray), Errors) {
  case binary {
    <<>> -> Ok(#(list.reverse(result), binary))
    <<"\n":utf8, rest:bytes>> ->
      parse_newline_terminated_string_rest(rest, result)
    _ -> Error(ParseError("parse_newline_terminated_string_start", binary))
  }
}

fn parse_newline_terminated_string_rest(
  binary: BitArray,
  result: List(BitArray),
) -> Result(#(List(BitArray), BitArray), Errors) {
  case binary {
    <<>> -> Ok(#(list.reverse(result), binary))
    <<"\n":utf8, rest:bytes>> -> Ok(#(list.reverse(result), rest))
    <<char:bytes-size(1)-unit(8), rest:bytes>> ->
      parse_newline_terminated_string_rest(rest, [char, ..result])
    _ -> Error(ParseError("parse_newline_terminated_string_rest_1", binary))
  }
}

fn txinfo_parser(binary: BitArray) {
  case binary {
    <<
      gmt_offset:big-size(4)-unit(8),
      is_dst:8-signed,
      abbrev_index:8-unsigned,
      rest:bytes,
    >> ->
      Ok(#(
        TransitionInfo(
          gmt_offset: gmt_offset,
          is_dst: is_dst == 1,
          abbrev_index: abbrev_index,
          starts_at: 0,
          abbreviation: "",
          is_std: False,
          is_utc: False,
        ),
        rest,
      ))
    _ -> Error(ParseError("txinfo_parser", binary))
  }
}

fn parse_array(
  binary: BitArray,
  count: Int,
  parser: fn(BitArray) -> Result(#(a, BitArray), Errors),
) -> Result(#(List(a), BitArray), Errors) {
  parse_array_rec(binary, count, parser, [])
}

fn parse_array_rec(
  binary: BitArray,
  count,
  parser,
  results,
) -> Result(#(List(a), BitArray), Errors) {
  case count {
    0 -> Ok(#(list.reverse(results), binary))
    _ ->
      parser(binary)
      |> result.then(fn(res: #(a, BitArray)) {
        parse_array_rec(res.1, count - 1, parser, [res.0, ..results])
      })
  }
}

fn uchar_parser(binary: BitArray) -> Result(#(Int, BitArray), Errors) {
  case binary {
    <<val:big-unsigned-int-size(1)-unit(8), rest:bytes>> -> Ok(#(val, rest))
    _ -> Error(ParseError("uchar_parser", binary))
  }
}

fn char_parser(binary: BitArray) -> Result(#(Int, BitArray), Errors) {
  case binary {
    <<val:int-size(1)-unit(8), rest:bytes>> -> Ok(#(val, rest))
    _ -> Error(ParseError("char_parser", binary))
  }
}

fn int32_parser(binary: BitArray) -> Result(#(Int, BitArray), Errors) {
  case binary {
    <<val:big-int-size(4)-unit(8), rest:bytes>> -> Ok(#(val, rest))
    _ -> Error(ParseError("int32_parser", binary))
  }
}

fn int_parser(
  version: Version,
) -> fn(BitArray) -> Result(#(Int, BitArray), Errors) {
  let size = case version {
    V1 -> 4
    V2 | V3 -> 8
  }

  fn(binary) {
    case binary {
      <<val:big-unsigned-int-size(size)-unit(8), rest:bytes>> ->
        Ok(#(val, rest))
      _ -> Error(ParseError("int_parser", binary))
    }
  }
}

fn parse_header(header: BitArray) {
  case header {
    <<
      isutcnt:big-unsigned-int-size(4)-unit(8),
      isstdcnt:big-unsigned-int-size(4)-unit(8),
      leapcnt:big-unsigned-int-size(4)-unit(8),
      timecnt:big-unsigned-int-size(4)-unit(8),
      typecnt:big-unsigned-int-size(4)-unit(8),
      charcnt:big-unsigned-int-size(4)-unit(8),
    >> ->
      Ok(Header(
        utc_count: isutcnt,
        wall_count: isstdcnt,
        leap_count: leapcnt,
        transition_count: timecnt,
        type_count: typecnt,
        abbrev_length: charcnt,
      ))
    _ -> Error(ParseError("parse_header", header))
  }
}
