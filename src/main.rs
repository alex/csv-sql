use clap::Parser;
use std::cmp::Ordering;
use std::fs::File;

fn normalize_col(col: &str) -> String {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(r"\(.*?\)$").unwrap();
    }
    let mut col = RE
        .replace_all(col, "")
        .to_lowercase()
        .trim()
        .replace('(', "")
        .replace(')', "")
        .replace(' ', "_")
        .replace('.', "_")
        .replace('-', "_")
        .replace('/', "_")
        .replace('?', "")
        .replace(',', "_")
        .replace('&', "_")
        .replace(':', "")
        .replace('#', "");
    if !col.chars().next().map(char::is_alphabetic).unwrap_or(true) {
        col = format!("c_{}", col)
    }
    col
}

fn _create_table(db: &mut rusqlite::Connection, table_name: &str, cols: &[String]) {
    let create_columns = cols
        .iter()
        .map(|c| format!("\"{}\" varchar", c))
        .collect::<Vec<String>>()
        .join(", ");
    db.execute(
        &format!("CREATE TABLE {} ({})", table_name, create_columns),
        &[] as &[&dyn rusqlite::types::ToSql],
    )
    .unwrap();
}

fn _load_table_from_path(
    db: &mut rusqlite::Connection,
    table_name: &str,
    path: &str,
    delimiter: u8,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut num_rows = 0;
    let f = File::open(path)?;
    let file_size = f.metadata().unwrap().len();
    let mut reader = csv::ReaderBuilder::new()
        .flexible(true)
        .delimiter(delimiter)
        .from_reader(f);

    let normalized_cols =
        reader
            .headers()?
            .iter()
            .map(normalize_col)
            .fold(vec![], |mut v, orig_col| {
                let mut col = orig_col.clone();
                let mut i = 1;
                while v.contains(&col) {
                    col = format!("{}_{}", orig_col, i);
                    i += 1
                }
                v.push(col);
                v
            });
    _create_table(db, table_name, &normalized_cols);

    let insert_query = format!(
        "INSERT INTO {} VALUES ({})",
        table_name,
        normalized_cols
            .iter()
            .map(|_| "?")
            .collect::<Vec<_>>()
            .join(", ")
    );
    let pb = indicatif::ProgressBar::new(file_size);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
            .progress_chars("#>-"),
    );
    let mut records = reader.byte_records();
    let tx = db.transaction().unwrap();
    {
        let mut stmt = tx.prepare(&insert_query).expect("tx.prepare() failed");
        while let Some(row) = records.next() {
            let mut row = row?;
            match row.len().cmp(&normalized_cols.len()) {
                Ordering::Less => {
                    for _ in 0..normalized_cols.len() - row.len() {
                        row.push_field(b"");
                    }
                }
                Ordering::Greater => {
                    panic!("Too many fields on row {}, fields: {:?}", num_rows + 1, row);
                }
                Ordering::Equal => {}
            }
            stmt.execute(rusqlite::params_from_iter(
                row.iter().map(String::from_utf8_lossy),
            ))
            .unwrap();

            num_rows += 1;
            if num_rows % 10000 == 0 {
                pb.set_position(records.reader().position().byte())
            }
        }
    }
    tx.commit().unwrap();
    pb.finish();

    println!(
        "Loaded {} rows into {}({}) from {:?}",
        num_rows,
        table_name,
        normalized_cols.join(", "),
        path,
    );
    Ok(normalized_cols)
}

struct FromAnySqlType {
    value: String,
}

impl rusqlite::types::FromSql for FromAnySqlType {
    fn column_result(
        value: rusqlite::types::ValueRef<'_>,
    ) -> Result<FromAnySqlType, rusqlite::types::FromSqlError> {
        let result = match value {
            rusqlite::types::ValueRef::Null => "null".to_string(),
            rusqlite::types::ValueRef::Integer(v) => v.to_string(),
            rusqlite::types::ValueRef::Real(v) => v.to_string(),
            rusqlite::types::ValueRef::Blob(v) | rusqlite::types::ValueRef::Text(v) => {
                String::from_utf8(v.to_vec()).unwrap()
            }
        };
        Ok(FromAnySqlType { value: result })
    }
}

fn _prepare_query<'a>(
    conn: &'a mut rusqlite::Connection,
    query: &str,
) -> Result<rusqlite::Statement<'a>, String> {
    conn.prepare(query).map_err(|e| e.to_string())
}

fn _handle_query(conn: &mut rusqlite::Connection, line: &str) -> Result<(), String> {
    let mut stmt = _prepare_query(conn, line)?;
    let col_count = stmt.column_count();

    let mut table = comfy_table::Table::new();
    table.load_preset("││──╞═╪╡┆    ┬┴┌┐└┘");
    table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
    let mut title_row = comfy_table::Row::new();
    for col in stmt.column_names() {
        title_row.add_cell(comfy_table::Cell::new(col));
    }
    table.set_header(title_row);

    let mut results = stmt.query(&[] as &[&dyn rusqlite::types::ToSql]).unwrap();
    while let Ok(Some(r)) = results.next() {
        let mut row = comfy_table::Row::new();
        for i in 0..col_count {
            let cell: FromAnySqlType = r.get(i).unwrap();
            row.add_cell(comfy_table::Cell::new(&cell.value));
        }
        table.add_row(row);
    }
    println!("{}", table);
    Ok(())
}

fn _handle_export(conn: &mut rusqlite::Connection, line: &str) -> Result<(), String> {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(r"^\.export\(([\w_\-\./]+)\) (.*)").unwrap();
    }
    let caps = RE
        .captures(line)
        .ok_or_else(|| "Must match `.export(file-name) SQL`".to_owned())?;
    let destination_path = &caps[1];
    let query = &caps[2];

    let mut stmt = _prepare_query(conn, query)?;
    let col_count = stmt.column_count();

    let mut writer = csv::Writer::from_path(destination_path).map_err(|e| format!("{:?}", e))?;
    writer.write_record(stmt.column_names()).unwrap();

    let mut results = stmt.query(&[] as &[&dyn rusqlite::types::ToSql]).unwrap();
    while let Ok(Some(r)) = results.next() {
        writer
            .write_record((0..col_count).map(|i| {
                let cell: FromAnySqlType = r.get(i).unwrap();
                cell.value
            }))
            .unwrap();
    }

    Ok(())
}

fn _process_query(conn: &mut rusqlite::Connection, line: &str) {
    let result = if line.starts_with(".export") {
        _handle_export(conn, line)
    } else if line.starts_with(".schema") {
        _handle_query(
            conn,
            "SELECT sql AS schema FROM sqlite_master WHERE name like 't%'",
        )
    } else {
        _handle_query(conn, line)
    };
    if let Err(e) = result {
        println!("{}", e);
    }
}

fn install_udfs(c: &mut rusqlite::Connection) -> Result<(), Box<dyn std::error::Error>> {
    c.create_scalar_function(
        "regexp_extract",
        3,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8
            | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let re = ctx.get_or_create_aux(
                0,
                |vr| -> Result<_, Box<dyn std::error::Error + Send + Sync + 'static>> {
                    Ok(regex::Regex::new(vr.as_str()?)?)
                },
            )?;
            let value = ctx.get::<Box<str>>(1)?;
            let replacement = ctx.get::<Box<str>>(2)?;

            let caps = match re.captures(&value) {
                Some(caps) => caps,
                None => return Ok("".to_string()),
            };
            let mut dest = String::new();
            caps.expand(&replacement, &mut dest);
            Ok(dest)
        },
    )?;
    c.create_scalar_function(
        "regexp",
        2,
        rusqlite::functions::FunctionFlags::SQLITE_UTF8
            | rusqlite::functions::FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let re = ctx.get_or_create_aux(
                0,
                |vr| -> Result<_, Box<dyn std::error::Error + Send + Sync + 'static>> {
                    Ok(regex::Regex::new(vr.as_str()?)?)
                },
            )?;
            let value = ctx.get::<Box<str>>(1)?;
            Ok(re.is_match(&value))
        },
    )?;

    Ok(())
}

struct SimpleWordCompleter {
    words: Vec<String>,
}

static BREAK_CHARS: [u8; 5] = [b' ', b'(', b')', b',', b'.'];
impl SimpleWordCompleter {
    fn new(words: Vec<String>) -> SimpleWordCompleter {
        SimpleWordCompleter { words }
    }
}

impl rustyline::Helper for SimpleWordCompleter {}

impl rustyline::hint::Hinter for SimpleWordCompleter {
    type Hint = String;

    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context<'_>) -> Option<String> {
        None
    }
}

impl rustyline::highlight::Highlighter for SimpleWordCompleter {}

impl rustyline::validate::Validator for SimpleWordCompleter {}

impl rustyline::completion::Completer for SimpleWordCompleter {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context<'_>,
    ) -> rustyline::Result<(usize, Vec<String>)> {
        let (start, word) = rustyline::completion::extract_word(line, pos, None, &BREAK_CHARS);

        let matches = self
            .words
            .iter()
            .filter(|w| w.starts_with(word))
            .cloned()
            .collect();
        Ok((start, matches))
    }
}

#[derive(clap::Parser)]
struct Opts {
    #[clap(long, about = "Use ',' as the delimiter for the CSV")]
    comma: bool,
    #[clap(long, about = "Use '|' as the delimiter for the CSV")]
    pipe: bool,
    #[clap(long, about = "Use '\\t' as the delimiter for the CSV")]
    tab: bool,
    #[clap(long, about = "Use ';' as the delimiter for the CSV")]
    semicolon: bool,

    #[clap()]
    paths: Vec<String>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = Opts::parse();

    let delim = match (opts.comma, opts.pipe, opts.tab, opts.semicolon) {
        (true, false, false, false) | (false, false, false, false) => b',',
        (false, true, false, false) => b'|',
        (false, false, true, false) => b'\t',
        (false, false, false, true) => b';',
        _ => {
            eprintln!("Can't pass more than one of --comma, --pipe, and --tab");
            std::process::exit(1);
        }
    };

    let mut conn = rusqlite::Connection::open_in_memory().unwrap();

    let mut base_words = [
        // keywords
        "distinct",
        "select",
        "from",
        "group",
        "by",
        "order",
        "where",
        "count",
        "limit",
        "offset",
        // functions
        "length",
        "coalesce",
        "regexp_extract",
        // csv-sql commands
        "export",
        "schema",
    ]
    .iter()
    .map(|&s| s.to_string())
    .collect::<Vec<String>>();

    if opts.paths.len() == 1 {
        let mut col_names = _load_table_from_path(&mut conn, "t", &opts.paths[0], delim)?;
        base_words.append(&mut col_names);
    } else {
        for (idx, path) in opts.paths.iter().enumerate() {
            let mut col_names =
                _load_table_from_path(&mut conn, &format!("t{}", idx + 1), path, delim)?;
            base_words.append(&mut col_names);
        }
    }

    install_udfs(&mut conn)?;

    let completer = SimpleWordCompleter::new(base_words);
    let mut rl = rustyline::Editor::new();
    rl.set_helper(Some(completer));
    let history_path = dirs::home_dir().unwrap().join(".csv-sql-history");
    let _ = rl.load_history(&history_path);
    loop {
        match rl.readline("> ") {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                _process_query(&mut conn, &line);
                rl.add_history_entry(line);
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("Interrupted");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {}", err);
                break;
            }
        }
    }
    rl.save_history(&history_path).unwrap();

    Ok(())
}

#[cfg(test)]
mod test {
    use super::normalize_col;

    #[test]
    fn test_normalize_col() {
        for (value, expected) in &[
            ("", ""),
            ("abc", "abc"),
            ("(S)AO", "sao"),
            ("abc (123)", "abc"),
            ("2/6/2000", "c_2_6_2000"),
            ("COMBO#", "combo"),
        ] {
            assert_eq!(&&normalize_col(value), expected);
        }
    }
}
