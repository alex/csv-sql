use std::fs::File;
use std::iter;

pub trait ExactSizeIterable {
    fn iter(&self) -> impl iter::ExactSizeIterator<Item = &[u8]>;
}

impl ExactSizeIterable for csv::ByteRecord {
    fn iter(&self) -> impl iter::ExactSizeIterator<Item = &[u8]> {
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
    fn raw_fields(&mut self) -> anyhow::Result<impl Iterator<Item = &str>>;

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

    fn raw_fields(&mut self) -> anyhow::Result<impl Iterator<Item = &str>> {
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
