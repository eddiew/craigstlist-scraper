open Core
open Async
open Cohttp
open Cohttp_async
open Soup.Infix

(** Borough specific queries - base/search/[code]/nfa where code is one of:
  * | "mnh" -> Manhattan
  * | "brk" -> Brooklyn
  * | "que" | "brx" | "stn" | "jsy" | "lgi" | "wch" | "fct" -> Don't care
  * Can also search with ?format=rss but you miss out on a lot of info, like prices *)
let url = "https://newyork.craigslist.org/search/nfa"

let base_listing_url = "https://newyork.craigslist.org"

let pg_host = "localhost"
let pg_port = 5432
let pg_db = Sys.getenv "SCRAPERDB" |> Option.value ~default:"scraperdb"
let pg_user = Sys.getenv "SCRAPERUSER" |> Option.value ~default:"scraperadmin"
let pg_pass = Sys.getenv "SCRAPERPASS" |> Option.value ~default:"scraperpassword"

(* Helper shit that really ought to go into a library somewhere *)

let expect_code code response =
  if Code.code_of_status (Response.status response) <> code then
    raise_s [%message "Expected response code" (code : int) ", got response " (response : Response.t)]

let download_page url =
  let%bind response, body = Client.get (Uri.of_string url) in
  expect_code 200 response;
  Body.to_string body

let parse_xml s =
  Markup.(string s |> parse_xml |> signals) |> Soup.from_signals

let parse_exn node ~f =
  f (Soup.R.leaf_text node)

let parse_bind node_opt ~f =
  let open Option.Monad_infix in
  node_opt >>= Soup.leaf_text >>= f

let parse_attribute_exn node ~attr ~f =
  f (Soup.R.attribute attr node)

let get_page url =
  download_page url >>| Soup.parse

let csv_escape' s = "\"" ^ (Option.value s ~default:"" |> String.escaped) ^ "\""
let csv_escape s = csv_escape' (Some s)

(* Hide the cancer (slightly) that is [Postgresql].
   Ensures the connection is not leaked, even in the case of an exception.
   Note: [f] is asynchronous, but really I should just be doing pg#exec because
   [Postgresql] is super barebones.
*)
let with_postgres_connection ~f =
  let port = string_of_int pg_port in
  let pg = new Postgresql.connection ~host:pg_host ~port ~dbname:pg_db ~user:pg_user ~password:pg_pass () in
  pg#set_notice_processor (fun s -> raise_s [%message "Postgres error: " (s : string)]);
  match%map try_with (fun () -> f pg) with
  | Ok res ->
    pg#finish;
    res
  | Error e ->
    pg#finish;
    raise e

(* Types and stuff *)

module Summary = struct
  module T = struct
    type t =
      { price: int
      ; title: string
      ; url_suffix: string
      ; beds: int
      ; sq_ft: int option
      ; neighborhood: string option
      ; post_time: Time_ns.t
      } [@@deriving fields, sexp]

    let parse_price str =
      String.slice str 1 0 |> int_of_string

    let parse_housing_exn node =
      let texts = Soup.trimmed_texts node in
      let parts =
        List.concat_map texts ~f:(String.split ~on:'-')
        |> List.filter ~f:(fun s -> not (String.is_empty s))
      in
      let beds =
        List.find_map parts ~f:(String.chop_suffix ~suffix:"br ")
        |> Option.value_exn ~message:"Could not find number of beds" |> int_of_string
      in
      let sq_ft =
        List.find_map parts ~f:(String.chop_suffix ~suffix:"ft")
        |> Option.map ~f:(fun s -> String.filter s ~f:Char.is_digit |> int_of_string)
      in
      beds, sq_ft

    let parse_hood str =
      String.strip str ~drop:(fun c -> c = '(' || c = ')' || c = ' ') |> Some

    let of_node node =
      try (
        let price = parse_exn (node $ ".result-price") ~f:parse_price in
        let title = parse_exn (node $ ".result-title") ~f:Fn.id in
        let url_suffix = parse_attribute_exn (node $ ".hdrlnk") ~attr:"href" ~f:Fn.id in
        (* If a ".housing" node is not present, the listing is usually a studio.
           I don't care about those anyway, so I'm dropping these. *)
        let beds, sq_ft = parse_housing_exn (node $ ".housing") in
        let neighborhood = parse_bind (node $? ".result-hood") ~f:parse_hood in
        let post_time = parse_attribute_exn (node $ ".result-date") ~attr:"datetime" ~f:Time_ns.of_string in
        Some { price; title; url_suffix; beds; sq_ft; neighborhood; post_time }
      )
      with
      | exn ->
        eprintf !"Warning: Failed to parse summary %{sexp: exn}\n" exn;
        None

    let print t =
      printf !"%{sexp:t}\n" t

    let csv_header =
      String.concat Fields.names ~sep:","

    let to_csv_string t =
      (* Fields.Direct.to_list t *)
      (*   ~price:(fun (_, _, price) -> int_of_string price) *)
      let price = string_of_int t.price |> csv_escape in
      let title = csv_escape t.title in
      let url = base_listing_url ^/ t.url_suffix |> csv_escape in
      let beds = string_of_int t.beds |> csv_escape in
      let sq_ft = Option.map t.sq_ft ~f:string_of_int |> csv_escape' in
      let neighborhood = csv_escape' t.neighborhood in
      let post_time = Time_ns.to_string t.post_time |> csv_escape in
      String.concat [price; title; url; beds; sq_ft; neighborhood; post_time] ~sep:","

    let to_csv_lines l =
      csv_header :: List.map l ~f:to_csv_string
  end
  include T
  include Sexpable.To_stringable (T)
end

module Listing = struct
  module T = struct
    type t =
      { summary : Summary.t
      ; latitude : float
      ; longitude : float
      ; location_accuracy : int
      ; description : string
      ; attributes : string list
      ; photos : string list
      } [@@deriving fields, sexp]

    let of_summary (summary : Summary.t) =
      let%map page = get_page (base_listing_url ^/ summary.url_suffix) in
      let body = page $ ".userbody" in
      match body $? "#has_been_removed" with
      | Some _ -> None
      | None ->
        try (
          let latitude, longitude, location_accuracy =
            let map = body $ "#map" in
            let latitude = Soup.R.attribute "data-latitude" map |> float_of_string in
            let longitude = Soup.R.attribute "data-longitude" map |> float_of_string in
            let location_accuracy = Soup.R.attribute "data-accuracy" map |> int_of_string in
            latitude, longitude, location_accuracy
          in
          let description =
            Soup.trimmed_texts (body $ "#postingbody") |> String.concat ~sep:"\n" in
          let attributes =
            let attr_nodes = body $$ ".attrgroup span" |> Soup.to_list in
            List.concat_map attr_nodes ~f:Soup.trimmed_texts
          in
          let photos =
            let nodes = body $$ ".iw.multiimage .thumb" |> Soup.to_list in
            List.map nodes ~f:(Soup.R.attribute "href")
          in
          Some { summary; latitude; longitude; location_accuracy; description; attributes; photos}
        )
        with
        | exn ->
          eprintf !"Warning: Failed to parse listing %{sexp: exn}\n" exn;
          None

    let csv_header =
      String.concat ~sep:"," (Summary.csv_header :: List.tl_exn Fields.names)

    let to_csv_string t =
      String.concat ~sep:","
        [ Summary.to_csv_string t.summary
        ; string_of_float t.latitude
        ; string_of_float t.longitude
        ; string_of_int t.location_accuracy
        ; csv_escape t.description
        ; String.concat ~sep:";" t.attributes |> csv_escape
        ; String.concat ~sep:";" t.photos |> csv_escape
        ]

    let to_csv_lines l =
      csv_header :: List.map l ~f:to_csv_string

    let upsert l pg =
      pg#exec "INSERT into listings "

  end
  include T
  include Sexpable.To_stringable (T)
end

(* Real code *)

(* TODO: get all pages *)
let download_summaries () =
  let%map page = get_page url in
  page $$ ".result-row" |> Soup.to_list |> List.filter_map ~f:Summary.of_node

let main () =
  let%bind summaries = download_summaries () in
  (* Run this serially instead of in parallel to rate limit ourselves *)
  let%map listings = Deferred.List.filter_map summaries ~f:Listing.of_summary (* ~how:`Parallel *) in
  let csv_lines = Listing.to_csv_lines listings in
  List.iter csv_lines ~f:print_endline

let () =
  let run =
    match%bind Monitor.try_with_or_error main with
    | Ok () -> exit 0
    | Error e ->
      eprintf !"Execution produced error: %{sexp: Error.t}\n" e;
      exit 1
  in
  don't_wait_for run;
  never_returns (Scheduler.go ())
