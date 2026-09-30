//! pkt-line framing (gitprotocol-common(5)), as used by git's long-running
//! filter process protocol. Codec only: no knowledge of filters.
//!
//! A packet is a 4-hex-digit length that counts itself, then the payload.
//! `0000` is a flush packet; `0001`–`0003` belong to protocol v2 and are
//! invalid here.

use std::fmt;
use std::io::{self, BufWriter, Read, Write};

/// Largest payload one packet can carry.
pub const MAX_DATA_LEN: usize = 65516;
const HEADER_LEN: usize = 4;
const MAX_PACKET_LEN: usize = MAX_DATA_LEN + HEADER_LEN;

/// A framing error: the stream cannot be trusted past this point.
#[derive(Debug)]
pub enum PktError {
    /// EOF inside a header or payload.
    Truncated,
    /// The length is not 4 hex digits.
    BadLength([u8; 4]),
    /// A protocol v2 special packet (delimiter, response end).
    Reserved(u16),
    /// Longer than the 65520 bytes a packet may be.
    TooLong(usize),
    Io(io::Error),
}

impl fmt::Display for PktError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Truncated => write!(f, "pkt-line stream ended inside a packet"),
            Self::BadLength(h) => write!(
                f,
                "pkt-line length is not 4 hex digits: {:?}",
                String::from_utf8_lossy(h)
            ),
            Self::Reserved(n) => write!(f, "reserved pkt-line length {n:04x}"),
            Self::TooLong(n) => write!(f, "pkt-line length {n} exceeds {MAX_PACKET_LEN}"),
            Self::Io(e) => write!(f, "pkt-line I/O error: {e}"),
        }
    }
}

impl std::error::Error for PktError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<io::Error> for PktError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

impl From<PktError> for io::Error {
    fn from(e: PktError) -> Self {
        match e {
            PktError::Io(e) => e,
            other => io::Error::new(io::ErrorKind::InvalidData, other),
        }
    }
}

/// Reads packets from `inner`, which should be buffered (a `StdinLock` is).
pub struct PktReader<R> {
    inner: R,
    /// The last data packet's payload; reused for every read.
    buf: Vec<u8>,
}

impl<R: Read> PktReader<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            buf: Vec::with_capacity(MAX_DATA_LEN),
        }
    }

    /// Text packets up to the next flush, each with one trailing LF
    /// stripped. Bytes, not strings: pathnames need not be UTF-8.
    /// `Ok(None)` is a clean EOF before the list starts.
    pub fn text_list(&mut self) -> Result<Option<Vec<Vec<u8>>>, PktError> {
        let mut lines = Vec::new();
        loop {
            match self.read_packet()? {
                None if lines.is_empty() => return Ok(None),
                None => return Err(PktError::Truncated),
                Some(Frame::Flush) => return Ok(Some(lines)),
                Some(Frame::Data) => {
                    let line = self.buf.strip_suffix(b"\n").unwrap_or(&self.buf[..]);
                    lines.push(line.to_vec());
                }
            }
        }
    }

    /// Data packets up to the next flush, read as one byte stream.
    pub fn content(&mut self) -> Content<'_, R> {
        Content {
            reader: self,
            pos: 0,
            len: 0,
            state: State::Reading,
        }
    }

    /// `Ok(None)` is a clean EOF before the first header byte: the peer shut
    /// down at a packet boundary.
    fn read_packet(&mut self) -> Result<Option<Frame>, PktError> {
        let mut header = [0u8; HEADER_LEN];
        match read_full(&mut self.inner, &mut header)? {
            0 => return Ok(None),
            HEADER_LEN => {}
            _ => return Err(PktError::Truncated),
        }
        // from_str_radix alone would accept a leading '+'.
        if !header.iter().all(u8::is_ascii_hexdigit) {
            return Err(PktError::BadLength(header));
        }
        let text = std::str::from_utf8(&header).map_err(|_| PktError::BadLength(header))?;
        let len = u16::from_str_radix(text, 16).map_err(|_| PktError::BadLength(header))?;
        match usize::from(len) {
            0 => Ok(Some(Frame::Flush)),
            1..=3 => Err(PktError::Reserved(len)),
            n if n > MAX_PACKET_LEN => Err(PktError::TooLong(n)),
            n => {
                self.buf.resize(n - HEADER_LEN, 0);
                if read_full(&mut self.inner, &mut self.buf)? < self.buf.len() {
                    return Err(PktError::Truncated);
                }
                Ok(Some(Frame::Data))
            }
        }
    }
}

enum Frame {
    Flush,
    /// Payload is in `PktReader::buf`.
    Data,
}

/// Read until `buf` is full or EOF; returns the bytes read.
fn read_full(reader: &mut impl Read, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(filled)
}

/// The payload of data packets up to a flush, as a [`Read`] stream that
/// returns 0 at the flush. Framing errors surface as `InvalidData`, and
/// every read after an error fails the same way: past a bad packet, payload
/// bytes could pass for headers, so the stream never resynchronises.
pub struct Content<'r, R> {
    reader: &'r mut PktReader<R>,
    pos: usize,
    len: usize,
    state: State,
}

enum State {
    Reading,
    /// The flush was read.
    Done,
    /// Reading failed; `io::Error` is not `Clone`, so its parts are kept.
    Failed(io::ErrorKind, String),
}

impl<R: Read> Content<'_, R> {
    /// Skip whatever is left, up to and including the flush, so the reader
    /// is at the next packet.
    pub fn drain(&mut self) -> io::Result<()> {
        io::copy(self, &mut io::sink()).map(drop)
    }

    /// The next frame, or the error that ends this content for good.
    fn next_frame(&mut self) -> io::Result<Frame> {
        let failure = match self.reader.read_packet() {
            Ok(Some(frame)) => return Ok(frame),
            Ok(None) => PktError::Truncated,
            Err(e) => e,
        };
        let err = io::Error::from(failure);
        self.state = State::Failed(err.kind(), err.to_string());
        Err(err)
    }
}

impl<R: Read> Read for Content<'_, R> {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        loop {
            match &self.state {
                State::Reading => {}
                State::Done => return Ok(0),
                State::Failed(kind, message) => return Err(io::Error::new(*kind, message.clone())),
            }
            if out.is_empty() {
                return Ok(0);
            }
            if self.pos < self.len {
                let n = out.len().min(self.len - self.pos);
                out[..n].copy_from_slice(&self.reader.buf[self.pos..self.pos + n]);
                self.pos += n;
                return Ok(n);
            }
            match self.next_frame()? {
                Frame::Flush => self.state = State::Done,
                Frame::Data => {
                    self.pos = 0;
                    self.len = self.reader.buf.len();
                }
            }
        }
    }
}

/// Writes packets through a buffer; [`PktWriter::send`] pushes out whatever
/// is still pending.
pub struct PktWriter<W: Write> {
    inner: BufWriter<W>,
    /// Pending content payload, emitted in full packets.
    data: Vec<u8>,
}

impl<W: Write> PktWriter<W> {
    pub fn new(inner: W) -> Self {
        Self {
            inner: BufWriter::with_capacity(MAX_PACKET_LEN, inner),
            data: Vec::with_capacity(MAX_DATA_LEN),
        }
    }

    /// One text packet: `line` plus LF.
    pub fn text(&mut self, line: &str) -> io::Result<()> {
        let len = HEADER_LEN + line.len() + 1;
        if len > MAX_PACKET_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "pkt-line text too long",
            ));
        }
        writeln!(self.inner, "{len:04x}{line}")
    }

    pub fn flush_pkt(&mut self) -> io::Result<()> {
        self.inner.write_all(b"0000")
    }

    /// A content stream: bytes written are sent in full packets, and
    /// [`ContentWriter::finish`] ends it with a flush.
    pub fn content(&mut self) -> ContentWriter<'_, W> {
        self.data.clear();
        ContentWriter { w: self }
    }

    /// Push everything buffered to the peer.
    pub fn send(&mut self) -> io::Result<()> {
        self.inner.flush()
    }

    fn emit_data(&mut self) -> io::Result<()> {
        write!(self.inner, "{:04x}", HEADER_LEN + self.data.len())?;
        self.inner.write_all(&self.data)?;
        self.data.clear();
        Ok(())
    }
}

/// See [`PktWriter::content`]. Never emits an empty data packet.
pub struct ContentWriter<'w, W: Write> {
    w: &'w mut PktWriter<W>,
}

impl<'w, W: Write> ContentWriter<'w, W> {
    /// Emit the remaining bytes, then a flush. Returns the writer for what
    /// follows the content.
    pub fn finish(self) -> io::Result<&'w mut PktWriter<W>> {
        if !self.w.data.is_empty() {
            self.w.emit_data()?;
        }
        self.w.flush_pkt()?;
        Ok(self.w)
    }
}

impl<W: Write> Write for ContentWriter<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        // Emit only once more data arrives, so content of exactly
        // MAX_DATA_LEN bytes is one packet.
        if self.w.data.len() == MAX_DATA_LEN {
            self.w.emit_data()?;
        }
        let n = buf.len().min(MAX_DATA_LEN - self.w.data.len());
        self.w.data.extend_from_slice(&buf[..n]);
        Ok(n)
    }

    /// Packets are only complete at [`ContentWriter::finish`].
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn reader(bytes: &[u8]) -> PktReader<Cursor<Vec<u8>>> {
        PktReader::new(Cursor::new(bytes.to_vec()))
    }

    /// The next packet: `Some(None)` is a flush, `None` a clean EOF.
    fn next(r: &mut PktReader<Cursor<Vec<u8>>>) -> Result<Option<Option<Vec<u8>>>, PktError> {
        Ok(r.read_packet()?.map(|frame| match frame {
            Frame::Flush => None,
            Frame::Data => Some(r.buf.clone()),
        }))
    }

    fn packets(bytes: &[u8]) -> Result<Vec<Option<Vec<u8>>>, PktError> {
        let mut r = reader(bytes);
        let mut out = Vec::new();
        while let Some(p) = next(&mut r)? {
            out.push(p);
        }
        Ok(out)
    }

    #[test]
    fn reads_the_gitprotocol_common_examples() {
        let got = packets(b"0006a\n0005a000bfoobar\n00040000").unwrap();
        assert_eq!(
            got,
            vec![
                Some(b"a\n".to_vec()),
                Some(b"a".to_vec()),
                Some(b"foobar\n".to_vec()),
                Some(Vec::new()),
                None,
            ]
        );
    }

    #[test]
    fn accepts_uppercase_hex() {
        let mut data = b"000B".to_vec();
        data.extend_from_slice(b"foobar\n");
        assert_eq!(packets(&data).unwrap(), vec![Some(b"foobar\n".to_vec())]);
    }

    #[test]
    fn binary_payload_is_untouched_and_text_list_strips_one_lf() {
        let got = packets(b"0008\0\n\n\0").unwrap();
        assert_eq!(got, vec![Some(b"\0\n\n\0".to_vec())]);

        let mut r = reader(b"0006a\n0005b0007c\n\n0000");
        assert_eq!(
            r.text_list().unwrap(),
            Some(vec![b"a".to_vec(), b"b".to_vec(), b"c\n".to_vec()])
        );
    }

    #[test]
    fn malformed_lengths_are_rejected() {
        assert!(matches!(packets(b"00g0"), Err(PktError::BadLength(h)) if &h == b"00g0"));
        assert!(matches!(packets(b"+00a"), Err(PktError::BadLength(_))));
        for (input, n) in [(b"0001", 1), (b"0002", 2), (b"0003", 3)] {
            assert!(matches!(packets(input), Err(PktError::Reserved(r)) if r == n));
        }
        assert!(matches!(packets(b"fff1"), Err(PktError::TooLong(0xfff1))));
    }

    #[test]
    fn eof_inside_a_packet_is_truncated_and_at_a_boundary_is_none() {
        assert!(matches!(packets(b"00"), Err(PktError::Truncated)));
        assert!(matches!(packets(b"000aabc"), Err(PktError::Truncated)));
        assert!(next(&mut reader(b"")).unwrap().is_none());
        assert_eq!(reader(b"").text_list().unwrap(), None);
        assert!(matches!(
            reader(b"0006a\n").text_list(),
            Err(PktError::Truncated)
        ));
    }

    #[test]
    fn content_reads_across_packets_and_stops_at_flush() {
        let mut r = reader(b"0007abc0004000adefghi00000006x\n");
        let mut out = Vec::new();
        r.content().read_to_end(&mut out).unwrap();
        assert_eq!(out, b"abcdefghi");
        assert_eq!(next(&mut r).unwrap(), Some(Some(b"x\n".to_vec())));
    }

    #[test]
    fn drain_leaves_the_reader_at_the_next_packet() {
        let mut r = reader(b"0007abc0007def00000006x\n");
        let mut content = r.content();
        let mut first = [0u8; 2];
        content.read_exact(&mut first).unwrap();
        content.drain().unwrap();
        assert_eq!(next(&mut r).unwrap(), Some(Some(b"x\n".to_vec())));
    }

    #[test]
    fn content_without_its_flush_is_invalid_data() {
        let mut r = reader(b"0007abc");
        let err = r.content().read_to_end(&mut Vec::new()).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    fn written(content: &[&[u8]]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut w = PktWriter::new(&mut out);
        let mut c = w.content();
        for chunk in content {
            c.write_all(chunk).unwrap();
        }
        c.finish().unwrap();
        w.send().unwrap();
        drop(w);
        out
    }

    #[test]
    fn empty_content_is_a_flush_only() {
        assert_eq!(written(&[]), b"0000");
        assert_eq!(written(&[b""]), b"0000");
    }

    #[test]
    fn content_is_split_at_max_data_len() {
        let full = vec![7u8; MAX_DATA_LEN];
        let mut expected = b"fff0".to_vec();
        expected.extend_from_slice(&full);
        expected.extend_from_slice(b"0000");
        assert_eq!(written(&[&full]), expected);

        let mut expected = b"fff0".to_vec();
        expected.extend_from_slice(&full);
        expected.extend_from_slice(b"0005\x070000");
        assert_eq!(written(&[&full, &[7]]), expected);
    }

    #[test]
    fn small_writes_are_merged_into_one_packet() {
        assert_eq!(written(&[b"ab", b"c", b"", b"de"]), b"0009abcde0000");
    }

    #[test]
    fn text_packets_end_in_lf() {
        let mut out = Vec::new();
        let mut w = PktWriter::new(&mut out);
        w.text("git-filter-server").unwrap();
        w.text("version=2").unwrap();
        w.flush_pkt().unwrap();
        w.send().unwrap();
        drop(w);
        assert_eq!(out, b"0016git-filter-server\n000eversion=2\n0000");
    }

    #[test]
    fn written_content_reads_back() {
        let data: Vec<u8> = (0..200_000u32).map(|i| (i % 251) as u8).collect();
        let bytes = written(&[&data]);
        let mut r = reader(&bytes);
        let mut back = Vec::new();
        r.content().read_to_end(&mut back).unwrap();
        assert_eq!(back, data);
        assert!(next(&mut r).unwrap().is_none());
    }
}
