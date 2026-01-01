use std::fs::File;

pub trait AsRef<'a, T>
where
    T: ?Sized,
{
    fn as_ref(&self) -> &'a T;
}

impl<'a> AsRef<'a, [u8]> for &'a [u8] {
    fn as_ref(&self) -> &'a [u8] {
        self
    }
}

pub trait Record {
    type Item<'a>: AsRef<'a, [u8]>
    where
        Self: 'a;

    type Iter<'a>: ExactSizeIterator<Item = Self::Item<'a>>
    where
        Self: 'a;

    fn iter(&self) -> Self::Iter<'_>;
}

impl Record for csv::ByteRecord {
    type Item<'a> = &'a [u8];
    type Iter<'a> = csv::ByteRecordIter<'a>;

    fn iter(&self) -> Self::Iter<'_> {
        self.into_iter()
    }
}

pub trait Loader {
    type RecordType: Record;

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
