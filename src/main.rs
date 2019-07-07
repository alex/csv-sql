use csv;
use indicatif;
use lazy_static;
use prettytable;
use regex;
use rusqlite;
use rustyline;
use std::env;
use std::error::Error;
use std::fs::File;

fn _normalize_col(col: &str) -> String {
    lazy_static::lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(r"\(.*?\)").unwrap();
    }
    RE.replace_all(col, "")
        .to_lowercase()
        .trim()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("?", "")
}

fn _create_table(db: &mut rusqlite::Connection, table_name: &str, cols: &[String]) {
    let create_columns = cols
        .iter()
        .map(|c| format!("{} varchar", c))
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
    path: String,
) -> Vec<String> {
    let mut num_rows = 0;
    let f = File::open(path).unwrap();
    let file_size = f.metadata().unwrap().len();
    let mut reader = csv::Reader::from_reader(f);

    let normalized_cols =
        reader
            .headers()
            .unwrap()
            .iter()
            .map(_normalize_col)
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
    let mut records = reader.records();
    let tx = db.transaction().unwrap();
    {
        let mut stmt = tx.prepare(&insert_query).unwrap();
        while let Some(row) = records.next() {
            stmt.execute(&row.unwrap()).unwrap();

            num_rows += 1;
            if num_rows % 10000 == 0 {
                pb.set_position(records.reader().position().byte())
            }
        }
    }
    tx.commit().unwrap();
    pb.finish();

    println!(
        "Loaded {} rows into {}({})",
        num_rows,
        table_name,
        normalized_cols.join(", "),
    );
    normalized_cols
}

struct FromAnySqlType {
    value: String,
}

impl rusqlite::types::FromSql for FromAnySqlType {
    fn column_result(
        value: rusqlite::types::ValueRef,
    ) -> Result<FromAnySqlType, rusqlite::types::FromSqlError> {
        let result = match value {
            rusqlite::types::ValueRef::Null => "null".to_string(),
            rusqlite::types::ValueRef::Integer(v) => v.to_string(),
            rusqlite::types::ValueRef::Real(v) => v.to_string(),
            rusqlite::types::ValueRef::Text(v) => v.to_string(),
            rusqlite::types::ValueRef::Blob(v) => String::from_utf8(v.to_vec()).unwrap(),
        };
        Ok(FromAnySqlType { value: result })
    }
}

fn _print_table(conn: &mut rusqlite::Connection, line: &str) {
    let mut table = prettytable::Table::new();
    table.set_format(*prettytable::format::consts::FORMAT_NO_LINESEP_WITH_TITLE);
    let mut stmt = match conn.prepare(&line) {
        Ok(stmt) => stmt,
        Err(e) => {
            println!("{}", e.description());
            return;
        }
    };

    let mut title_row = prettytable::Row::new(vec![]);
    for col in stmt.column_names() {
        title_row.add_cell(prettytable::Cell::new(col));
    }
    table.set_titles(title_row);

    let mut results = stmt.query(&[] as &[&dyn rusqlite::types::ToSql]).unwrap();
    while let Ok(Some(r)) = results.next() {
        let mut row = prettytable::Row::new(vec![]);
        for i in 0..r.column_count() {
            let cell: FromAnySqlType = r.get(i).unwrap();
            row.add_cell(prettytable::Cell::new(&cell.value));
        }
        table.add_row(row);
    }
    table.printstd();
}

struct SimpleWordCompleter {
    words: Vec<String>,
}

static BREAK_CHARS: [u8; 4] = [b' ', b'(', b')', b','];
impl SimpleWordCompleter {
    fn new(words: Vec<String>) -> SimpleWordCompleter {
        SimpleWordCompleter { words }
    }
}

impl rustyline::Helper for SimpleWordCompleter {}

impl rustyline::hint::Hinter for SimpleWordCompleter {
    fn hint(&self, _line: &str, _pos: usize, _ctx: &rustyline::Context) -> Option<String> {
        None
    }
}

impl rustyline::highlight::Highlighter for SimpleWordCompleter {}

impl rustyline::completion::Completer for SimpleWordCompleter {
    type Candidate = String;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &rustyline::Context,
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

fn main() {
    let mut paths = env::args().skip(1);

    let mut conn = rusqlite::Connection::open_in_memory().unwrap();

    let mut base_words = vec![
        "distinct", "select", "from", "group", "by", "order", "where", "count", "limit", "offset",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect::<Vec<String>>();

    if paths.len() == 1 {
        let mut col_names = _load_table_from_path(&mut conn, "t", paths.next().unwrap());
        base_words.append(&mut col_names);
    } else {
        for (idx, path) in paths.enumerate() {
            let mut col_names = _load_table_from_path(&mut conn, &format!("t{}", idx + 1), path);
            base_words.append(&mut col_names);
        }
    }

    let completer = SimpleWordCompleter::new(base_words);
    let mut rl = rustyline::Editor::new();
    rl.set_helper(Some(completer));
    loop {
        match rl.readline("> ") {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                rl.add_history_entry(line.clone());
                _print_table(&mut conn, &line);
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
}
