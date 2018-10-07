extern crate csv;
#[macro_use]
extern crate lazy_static;
extern crate prettytable;
extern crate regex;
extern crate rusqlite;
extern crate rustyline;

use std::env;
use std::error::Error;


fn _normalize_col(col: &str) -> String {
    lazy_static! {
        static ref RE: regex::Regex = regex::Regex::new(r"\(.*?\)").unwrap();
    }
    return RE.replace_all(col, "")
        .to_lowercase()
        .trim()
        .replace(" ", "_")
        .replace(".", "_")
        .replace("?", "");
}

fn _create_table(db: &mut rusqlite::Connection, table_name: &str, cols: &Vec<String>) {
    let create_columns = cols.iter()
        .map(|c| format!("{} varchar", c))
        .collect::<Vec<String>>()
        .join(", ");
    db.execute(&format!("CREATE TABLE {} ({})", table_name, create_columns), &[])
        .unwrap();
}

fn _insert_row(
    db: &mut rusqlite::Connection,
    table_name: &str,
    row: Vec<String>,
    cols: &Vec<String>,
) {
    let placeholders = cols.iter()
        .map(|_| "?")
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = db.prepare(&format!(
        "INSERT INTO {} VALUES ({})",
        table_name,
        placeholders
    )).unwrap();

    let params = row.iter().map(|p| p as &rusqlite::types::ToSql).collect::<Vec<&rusqlite::types::ToSql>>();
    let res = stmt.execute(&params);
    assert!(res.is_ok());
}

fn _load_table_from_path(
    db: &mut rusqlite::Connection,
    table_name: &str,
    path: String,
) -> Vec<String> {
    let mut num_rows = 0;
    let mut reader = csv::Reader::from_path(path).unwrap();

    let normalized_cols = reader
        .headers()
        .unwrap()
        .iter()
        .map(_normalize_col)
        .collect();
    _create_table(db, table_name, &normalized_cols);

    for row in reader.records() {
        _insert_row(db, table_name, row.unwrap().iter().map(|s| s.to_string()).collect(), &normalized_cols);
        num_rows += 1;
    }

    println!(
        "Loaded {} rows into {}({})",
        num_rows,
        table_name,
        normalized_cols.join(", "),
    );
    return normalized_cols;
}

struct FromAnySqlType {
    value: String,
}

impl rusqlite::types::FromSql for FromAnySqlType {
    fn column_result(value: rusqlite::types::ValueRef) -> Result<FromAnySqlType, rusqlite::types::FromSqlError> {
        let result = match value {
            rusqlite::types::ValueRef::Null => "null".to_string(),
            rusqlite::types::ValueRef::Integer(v) => v.to_string(),
            rusqlite::types::ValueRef::Real(v) => v.to_string(),
            rusqlite::types::ValueRef::Text(v) => v.to_string(),
            rusqlite::types::ValueRef::Blob(v) => String::from_utf8(v.to_vec()).unwrap()
        };
        Ok(FromAnySqlType{value: result})
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
    let mut results = stmt.query(&[]).unwrap();
    while let Some(Ok(r)) = results.next() {
        let mut row = prettytable::Row::new(vec![]);
        for i in 0..r.column_count() {
            let cell: FromAnySqlType = r.get(i);
            row.add_cell(prettytable::Cell::new(&cell.value));
        }
        table.add_row(row);
    }
    table.printstd();
}

struct SimpleWordCompleter {
    words: Vec<String>,
}


static BREAK_CHARS: [char; 4] = [' ', '(', ')', ','];
impl SimpleWordCompleter {
    fn new(words: Vec<String>) -> SimpleWordCompleter {
        return SimpleWordCompleter { words: words };
    }
}

impl rustyline::completion::Completer for SimpleWordCompleter {
    fn complete(&self, line: &str, pos: usize) -> rustyline::Result<(usize, Vec<String>)> {
        let (start, word) =
            rustyline::completion::extract_word(line, pos, &BREAK_CHARS.iter().cloned().collect());

        let matches = self.words
            .iter()
            .filter(|w| w.starts_with(word))
            .cloned()
            .collect();
        return Ok((start, matches));
    }
}

fn main() {
    let mut paths = env::args().skip(1);

    let mut conn = rusqlite::Connection::open_in_memory().unwrap();

    let mut base_words = vec![
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
    ].iter()
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
    rl.set_completer(Some(completer));
    loop {
        match rl.readline("> ") {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                rl.add_history_entry(&line);
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
