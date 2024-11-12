use clap::Parser;
use std::iter;
use std::sync::LazyLock;

use csvsql::ExactSizeIterable;

fn normalize_col(col: &str) -> String {
    static RE: LazyLock<regex::Regex> = LazyLock::new(|| regex::Regex::new(r"\(.*?\)$").unwrap());

    let mut col = RE
        .replace_all(col, "")
        .to_lowercase()
        .trim()
        .replace(['(', ')'], "")
        .replace([' ', '.', '-', '/'], "_")
        .replace('?', "")
        .replace([',', '&'], "_")
        .replace([':', '#'], "");
    if !col.chars().next().map(char::is_alphabetic).unwrap_or(true) {
        col = format!("c_{col}")
    }
    col
}

fn _create_table(db: &rusqlite::Connection, table_name: &str, cols: &[String]) {
    let create_columns = cols
        .iter()
        .map(|c| format!("\"{c}\" varchar"))
        .collect::<Vec<String>>()
        .join(", ");
    db.execute(
        &format!("CREATE TABLE {table_name} ({create_columns})"),
        &[] as &[&dyn rusqlite::types::ToSql],
    )
    .unwrap();
}

fn _load_table_from_path(
    db: &mut rusqlite::Connection,
    table_name: &str,
    path: &str,
    delimiter: u8,
) -> anyhow::Result<Vec<String>> {
    let loader = csvsql::CsvLoader::new(path, delimiter)?;

    _load_table_from_loader(db, table_name, loader)
}

fn _load_table_from_loader(
    db: &mut rusqlite::Connection,
    table_name: &str,
    mut loader: impl csvsql::Loader,
) -> anyhow::Result<Vec<String>> {
    let mut num_rows = 0;
    let progress_size = loader.progress_size();

    let normalized_cols = loader
        .raw_fields()
        .iter()
        .map(|v| normalize_col(v.as_ref()))
        .fold(vec![], |mut v, orig_col| {
            let mut col = orig_col.clone();
            let mut i = 1;
            while v.contains(&col) {
                col = format!("{orig_col}_{i}");
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
    let pb = indicatif::ProgressBar::new(progress_size);
    pb.set_style(
        indicatif::ProgressStyle::default_bar()
            .template("[{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-"),
    );
    let tx = db.transaction().unwrap();
    {
        let mut stmt = tx.prepare(&insert_query).expect("tx.prepare() failed");
        while let Some(record) = loader.next_record() {
            let record = record?;
            let row = record.iter();
            let row_len = row.len();
            if row_len > normalized_cols.len() {
                anyhow::bail!(
                    "Too many fields on row {}, fields: {:?}",
                    num_rows + 1,
                    row.collect::<Vec<_>>()
                );
            }

            stmt.execute(rusqlite::params_from_iter(
                row.chain(iter::repeat(&b""[..]).take(normalized_cols.len() - row_len))
                    .map(String::from_utf8_lossy),
            ))
            .unwrap();

            num_rows += 1;
            if num_rows % 10000 == 0 {
                pb.set_position(loader.progress_position());
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
        loader.name(),
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
    conn: &'a rusqlite::Connection,
    query: &str,
) -> anyhow::Result<rusqlite::Statement<'a>> {
    Ok(conn.prepare(query)?)
}

fn _handle_query(
    conn: &rusqlite::Connection,
    line: &str,
    style: &OutputStyle,
) -> anyhow::Result<()> {
    let mut stmt = _prepare_query(conn, line)?;
    let col_count = stmt.column_count();
    let col_names = stmt
        .column_names()
        .into_iter()
        .map(|s| s.to_owned())
        .collect::<Vec<_>>();
    let mut results = stmt.query(&[] as &[&dyn rusqlite::types::ToSql]).unwrap();

    match style {
        OutputStyle::Table => {
            let mut table = comfy_table::Table::new();
            table.load_preset("││──╞═╪╡┆    ┬┴┌┐└┘");
            table.set_content_arrangement(comfy_table::ContentArrangement::Dynamic);
            let mut title_row = comfy_table::Row::new();
            for col in col_names {
                title_row.add_cell(comfy_table::Cell::new(col));
            }
            table.set_header(title_row);

            while let Ok(Some(r)) = results.next() {
                let mut row = comfy_table::Row::new();
                for i in 0..col_count {
                    let cell: FromAnySqlType = r.get(i).unwrap();
                    row.add_cell(comfy_table::Cell::new(cell.value));
                }
                table.add_row(row);
            }
            println!("{table}");
        }
        OutputStyle::Vertical => {
            let max_col_length = col_names.iter().map(|c| c.len()).max().unwrap();
            let mut record_number = 1;
            while let Ok(Some(r)) = results.next() {
                println!("------ [ RECORD {record_number} ] ------");
                for (i, name) in col_names.iter().enumerate() {
                    let cell: FromAnySqlType = r.get(i).unwrap();
                    println!(
                        "{field:width$} | {value}",
                        field = name,
                        width = max_col_length,
                        value = cell.value
                    );
                }
                record_number += 1;
            }
        }
    }

    Ok(())
}

fn _handle_export(conn: &rusqlite::Connection, line: &str) -> anyhow::Result<()> {
    static RE: LazyLock<regex::Regex> =
        LazyLock::new(|| regex::Regex::new(r"^\.export\(([\w_\-\./]+)\) (.*)").unwrap());

    let caps = RE
        .captures(line)
        .ok_or_else(|| anyhow::anyhow!("Must match `.export(file-name) SQL`"))?;
    let destination_path = &caps[1];
    let query = &caps[2];

    let mut stmt = _prepare_query(conn, query)?;
    let col_count = stmt.column_count();

    let mut writer = csv::Writer::from_path(destination_path)?;
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

fn _process_query(conn: &rusqlite::Connection, line: &str, style: &mut OutputStyle) {
    let result = if line.starts_with(".export") {
        _handle_export(conn, line)
    } else if line.starts_with(".schema") {
        _handle_query(
            conn,
            "SELECT sql AS schema FROM sqlite_master WHERE name like 't%'",
            style,
        )
    } else if line.starts_with(".style") {
        static RE: LazyLock<regex::Regex> =
            LazyLock::new(|| regex::Regex::new(r"^\.style\((table|vertical)\)").unwrap());

        match RE.captures(line).as_ref().map(|caps| &caps[1]) {
            Some("table") => {
                *style = OutputStyle::Table;
                Ok(())
            }
            Some("vertical") => {
                *style = OutputStyle::Vertical;
                Ok(())
            }
            _ => Err(anyhow::anyhow!("Must match `.style(table|vertical)`")),
        }
    } else {
        _handle_query(conn, line, style)
    };
    if let Err(e) = result {
        println!("{e:?}");
    }
}

fn install_udfs(c: &rusqlite::Connection) -> anyhow::Result<()> {
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

static BREAK_CHARS: [char; 5] = [' ', '(', ')', ',', '.'];
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
        let (start, word) =
            rustyline::completion::extract_word(line, pos, None, |c| BREAK_CHARS.contains(&c));

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
    #[clap(long, help = "Use ',' as the delimiter for the CSV")]
    comma: bool,
    #[clap(long, help = "Use '|' as the delimiter for the CSV")]
    pipe: bool,
    #[clap(long, help = "Use '\\t' as the delimiter for the CSV")]
    tab: bool,
    #[clap(long, help = "Use ';' as the delimiter for the CSV")]
    semicolon: bool,

    #[clap()]
    paths: Vec<String>,
}

enum OutputStyle {
    Table,
    Vertical,
}

fn main() -> anyhow::Result<()> {
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
        "group_concat",
        // csv-sql commands
        "export",
        "schema",
        "style",
        "table",
        "vertical",
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

    install_udfs(&conn)?;

    let mut style = OutputStyle::Table;
    let completer = SimpleWordCompleter::new(base_words);
    let mut rl = rustyline::Editor::new()?;
    rl.set_helper(Some(completer));
    let history_path = dirs::home_dir().unwrap().join(".csv-sql-history");
    let _ = rl.load_history(&history_path);
    loop {
        match rl.readline("> ") {
            Ok(line) => {
                if line.trim().is_empty() {
                    continue;
                }
                _process_query(&conn, &line, &mut style);
                let _ = rl.add_history_entry(line);
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                println!("Interrupted");
                continue;
            }
            Err(rustyline::error::ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                println!("Error: {err}");
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
