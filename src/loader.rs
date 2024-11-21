use std::fs::File;
use std::iter;

use calamine::Reader;

pub trait ExactSizeIterable {
    fn iter(&self) -> impl iter::ExactSizeIterator<Item = impl AsRef<[u8]>>;
}

impl ExactSizeIterable for csv::ByteRecord {
    fn iter(&self) -> impl iter::ExactSizeIterator<Item = impl AsRef<[u8]>> {
        self.into_iter()
    }
}

pub trait Loader {
    type RecordType: ExactSizeIterable;

    /// Name of the resource we're loading from (e.g., a file path).
    fn name(&self) -> &str;

    /// Returns the size of the data of the loader, in unspecified units.
    /// Should be used for showing progress bars and similar.
    fn progress_size(&self) -> u64;

    /// Returns the current position of the loader relative to `progress_size`,
    /// in unspecified units.
    /// Should be used for showing progress bars and similar.
    fn progress_position(&self) -> u64;

    /// Returns the names of fields, as they exist in the underlying data.
    fn raw_fields(&mut self) -> anyhow::Result<impl Iterator<Item = impl AsRef<str>>>;

    fn next_record(&mut self) -> Option<anyhow::Result<Self::RecordType>>;
}

pub struct CsvLoader<'a> {
    path: &'a str,
    records: csv::ByteRecordsIntoIter<File>,
}

impl<'a> CsvLoader<'a> {
    pub fn new(path: &'a str, delimiter: u8) -> anyhow::Result<Self> {
        let f = File::open(path)?;

        let reader = csv::ReaderBuilder::new()
            .flexible(true)
            .delimiter(delimiter)
            .from_reader(f);

        Ok(CsvLoader {
            path,
            records: reader.into_byte_records(),
        })
    }
}

impl Loader for CsvLoader<'_> {
    type RecordType = csv::ByteRecord;

    fn name(&self) -> &str {
        self.path
    }

    fn progress_size(&self) -> u64 {
        self.records.reader().get_ref().metadata().unwrap().len()
    }

    fn progress_position(&self) -> u64 {
        self.records.reader().position().byte()
    }

    fn raw_fields(&mut self) -> anyhow::Result<impl Iterator<Item = impl AsRef<str>>> {
        Ok(self.records.reader_mut().headers()?.iter())
    }

    fn next_record(&mut self) -> Option<anyhow::Result<Self::RecordType>> {
        match self.records.next() {
            Some(Ok(v)) => Some(Ok(v)),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        }
    }
}

pub struct XlsxLoader<'a> {
    path: &'a str,
    data_range: calamine::Range<calamine::Data>,
    pos: usize
}

impl<'a> XlsxLoader<'a> {
    pub fn new(path: &'a str) -> anyhow::Result<Self> {
        let mut wb: calamine::Xlsx<_> = calamine::open_workbook(path)?;
        let data_range = wb
            .worksheet_range_at(0)
            .ok_or_else(|| anyhow::anyhow!("No worksheet in xlsx"))??;

        Ok(XlsxLoader { path, data_range, pos: 0 })
    }
}

pub struct XlsxRecord(Vec<Vec<u8>>);

impl ExactSizeIterable for XlsxRecord {
    fn iter(&self) -> impl iter::ExactSizeIterator<Item = impl AsRef<[u8]>> {
        self.0.iter()
    }
}

impl Loader for XlsxLoader<'_> {
    type RecordType = XlsxRecord;

    fn name(&self) -> &str {
        self.path
    }

    fn progress_size(&self) -> u64 {
        self.data_range.rows().len().try_into().unwrap()
    }

    fn progress_position(&self) -> u64 {
        todo!()
    }

    fn raw_fields(&mut self) -> anyhow::Result<impl Iterator<Item = impl AsRef<str>>> {
        Ok(self
            .data_range
            .headers()
            .ok_or_else(|| anyhow::anyhow!("No rows in xlsx"))?
            .into_iter())
    }

    fn next_record(&mut self) -> Option<anyhow::Result<Self::RecordType>> {
        self.pos += 1;
        let row = self.data_range.rows().skip(self.pos).next()?;
        let record = XlsxRecord(row.iter().map(|v| {
            b"abc".to_vec()
        }).collect());
        Some(Ok(record))
    }
}
