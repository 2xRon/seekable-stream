using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Linq;

namespace SeekableStream;

public class CachingSeekableStream : Stream
{
    const long DEFAULT_PAGE_LENGTH = 25 * 1024 * 1024;
    const int DEFAULT_MAX_PAGE_COUNT = 20;

    private readonly IRangeStreamAccessor _accessor;
    private readonly long _pageSize = DEFAULT_PAGE_LENGTH;
    private readonly long _maxPages = DEFAULT_MAX_PAGE_COUNT;
    private readonly CancellationTokenSource _disposeToken;
    private readonly RangeStreamWriter _writer = new();

    // Cached data
    private readonly Dictionary<long, byte[]> Pages = new(DEFAULT_MAX_PAGE_COUNT);

    // Track page access frequency so we preferentially drop unused pages
    private readonly Dictionary<long, long> HotList = new(DEFAULT_MAX_PAGE_COUNT);

    public long TotalRead { get; private set; }
    public long TotalLoaded { get; private set; }

    public CachingSeekableStream(IRangeStreamAccessor rangeStreamAccessor, long page = DEFAULT_PAGE_LENGTH, int maxPages = DEFAULT_MAX_PAGE_COUNT)
    {
        _accessor = rangeStreamAccessor;
        _pageSize = page;
        _maxPages = maxPages;
        _disposeToken = new CancellationTokenSource();
        Length = _accessor.Length;
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            try { _disposeToken.Cancel(); } catch { }
            try { _disposeToken.Dispose(); } catch { }
        }
        base.Dispose(disposing);
    }

    public override bool CanRead => true;
    public override bool CanSeek => true;
    public override bool CanWrite => false;

    public override long Length { get; }

    private long _position;
    public override long Position
    {
        get => _position;
        set => Seek(value, value >= 0 ? SeekOrigin.Begin : SeekOrigin.End);
    }

#if NETCOREAPP
    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken)
    {
        if (_position < 0 || _position >= Length)
            return 0;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken);
        long pos = _position;
        int totalRead = 0;
        do
        {
            long pageIndex = pos / _pageSize;
            long pageOffset = pos % _pageSize;

            if (!Pages.TryGetValue(pageIndex, out byte[]? pageData))
            {
                // if we have too many pages, drop the coolest
                while (Pages.Count >= _maxPages)
                {
                    var trim = Pages.OrderBy(kv => HotList[kv.Key]).First().Key;
                    Pages.Remove(trim);
                }

                long start = pageIndex * _pageSize;
                long end = start + Math.Min(_pageSize, Length - start); // read in a single page (we're looping)

                Pages[pageIndex] = (pageData = new byte[end - start]);

                if (!HotList.ContainsKey(pageIndex))
                    HotList[pageIndex] = 1;

                _writer.Buffer = pageData;
                TotalLoaded += await _accessor.ReadRangeAsync(start, end, _writer.WriteAsync, cts.Token);
            }
            else HotList[pageIndex] += 1;

            int toCopy = (int)Math.Min(pageData.Length - pageOffset, buffer.Length);
            pageData.AsSpan((int)pageOffset, toCopy).CopyTo(buffer.Span);
            buffer = buffer[toCopy..]; // Move the buffer slice forward
            totalRead += toCopy;
            pos += toCopy;
        } while (buffer.Length > 0 && pos < Length);

        long c = pos - _position;
        TotalRead += c;
        _position = pos;
        return (int)c;
    }
#endif

    public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_position < 0 || _position >= Length)
            return 0;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(_disposeToken.Token, cancellationToken);
        long pos = _position;
        do
        {
            long pageIndex = pos / _pageSize;
            long pageOffset = pos % _pageSize;

            if (!Pages.TryGetValue(pageIndex, out byte[]? pageData))
            {
                // if we have too many pages, drop the coolest
                while (Pages.Count >= _maxPages)
                {
                    var trim = Pages.OrderBy(kv => HotList[kv.Key]).First().Key;
                    Pages.Remove(trim);
                }

                long start = pageIndex * _pageSize;
                long end = start + Math.Min(_pageSize, Length - start); // read in a single page (we're looping)

                Pages[pageIndex] = (pageData = new byte[end - start]);

                if (!HotList.ContainsKey(pageIndex))
                    HotList[pageIndex] = 1;

                _writer.Buffer = pageData;
                TotalLoaded += await _accessor.ReadRangeAsync(start, end, _writer.WriteAsync, cts.Token);
            }
            else HotList[pageIndex] += 1;

            long l = Math.Min(pageData.Length - pageOffset, count);
            Array.Copy(pageData, (int)pageOffset, buffer, offset, (int)l);
            offset += (int)l;
            count -= (int)l;
            pos += (int)l;
        } while (count > 0 && pos < Length);

        long c = pos - _position;
        TotalRead += c;
        _position = pos;
        return (int)c;
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        if (_position < 0 || _position >= Length)
            return 0;

        long pos = _position;
        do
        {
            long pageIndex = pos / _pageSize;
            long pageOffset = pos % _pageSize;

            if (!Pages.TryGetValue(pageIndex, out byte[]? pageData))
            {
                // if we have too many pages, drop the coolest
                while (Pages.Count >= _maxPages)
                {
                    var trim = Pages.OrderBy(kv => HotList[kv.Key]).First().Key;
                    Pages.Remove(trim);
                }

                long start = pageIndex * _pageSize;
                long end = start + Math.Min(_pageSize, Length - start); // read in a single page (we're looping)

                Pages[pageIndex] = (pageData = new byte[end - start]);
                if (!HotList.ContainsKey(pageIndex))
                    HotList[pageIndex] = 1;

                _writer.Buffer = pageData;
                TotalLoaded += _accessor.ReadRange(start, end, _writer.Write, _disposeToken.Token);
            }
            else HotList[pageIndex] += 1;

            long l = Math.Min(pageData.Length - pageOffset, count);
            Array.Copy(pageData, (int)pageOffset, buffer, offset, (int)l);
            offset += (int)l;
            count -= (int)l;
            pos += (int)l;
        } while (count > 0 && pos < Length);

        long c = pos - _position;
        TotalRead += c;
        _position = pos;
        return (int)c;
    }

    private class RangeStreamWriter
    {
        public byte[] Buffer = null!;

        public int Write(Stream sourceStream)
        {
            int read = 0;
            do
            {
                read += sourceStream.Read(Buffer, read, Buffer.Length - read);
            } while (read < Buffer.Length);
            return read;
        }

        public async Task<int> WriteAsync(Stream sourceStream, CancellationToken cancellationToken)
        {
            int read = 0;
            do
            {
#if NETCOREAPP
                read += await sourceStream.ReadAsync(Buffer.AsMemory(read, Buffer.Length - read), cancellationToken);
#else
                read += await sourceStream.ReadAsync(Buffer, read, Buffer.Length - read, cancellationToken);
#endif
            } while (read < Buffer.Length);
            return read;
        }
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        var newPos = _position;
        switch (origin)
        {
            case SeekOrigin.Begin:
                newPos = offset; // offset must be positive
                break;
            case SeekOrigin.Current:
                newPos += offset; // + or -
                break;
            case SeekOrigin.End:
                newPos = Length - Math.Abs(offset); // offset must be negative?
                break;
        }
        if (newPos < 0 || newPos > Length)
            throw new InvalidOperationException("Stream position is invalid.");
        return _position = newPos;
    }

    public override void SetLength(long value) => throw new NotImplementedException();
    public override void Write(byte[] buffer, int offset, int count) => throw new NotImplementedException();
    public override void Flush() => throw new NotImplementedException();
}
