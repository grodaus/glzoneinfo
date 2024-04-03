import gleam/list
import gleam/result
import simplifile
import pprint
import glzoneinfo
import showtime
import showtime/tests/should
import showtime/tests/do_test.{do_test}
import showtime/tests/meta

pub fn main() {
  showtime.main()
}

fn all_zone_files(path: String, files: List(String)) {
  case simplifile.verify_is_directory(path) {
    Ok(True) ->
      case simplifile.read_directory(path) {
        Ok(paths) ->
          list.map(paths, fn(p) { all_zone_files(path <> "/" <> p, files) })
          |> list.flatten()
        Error(_) -> [path, ..files]
      }
    Ok(False) -> [path, ..files]
    Error(_) -> files
  }
}

pub fn load_meta_test() {
  "/etc/zoneinfo"
  |> all_zone_files([])
  |> list.each(fn(filename) {
    case filename {
      "/etc/zoneinfo/leapseconds"
      | "/etc/zoneinfo/posix/iso3166.tab"
      | "/etc/zoneinfo/posix/zone.tab"
      | "/etc/zoneinfo/posix/leapseconds"
      | "/etc/zoneinfo/posix/zone1970.tab"
      | "/etc/zoneinfo/posix/tzdata.zi"
      | "/etc/zoneinfo/iso3166.tab"
      | "/etc/zoneinfo/zone1970.tab"
      | "/etc/zoneinfo/tzdata.zi"
      | "/etc/zoneinfo/zone.tab" -> Nil
      _ -> {
        do_test(meta.Meta("Parsing " <> filename, ["parsing"]), fn(meta) {
          from_file(filename)
          |> do_test.be_ok(meta)
        })
        Nil
      }
    }
  })
}

pub fn parse_test() {
  from_file("/etc/zoneinfo/Europe/Vienna")
  |> should.be_ok()
}

fn from_file(filename: String) {
  use binary <- result.try(
    simplifile.read_bits(filename)
    |> result.map_error(fn(e) { FileError(e) }),
  )
  glzoneinfo.parse(binary)
  |> result.map_error(fn(e) { GltzError(e) })
}

type Errors {
  FileError(simplifile.FileError)
  GltzError(glzoneinfo.Errors)
}
