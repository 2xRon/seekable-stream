using System.Threading.Tasks;
using System.Threading;
using System.IO;

namespace SeekableStream;

public delegate int RangeWriter(Stream sourceStream);
public delegate Task<int> RangeWriterAsync(Stream sourceStream, CancellationToken cancellationToken);

public interface IRangeStreamAccessor
{
    long Length { get; }
    Task<int> ReadRangeAsync(long start, long end, RangeWriterAsync writer, CancellationToken cancellationToken);
    int ReadRange(long start, long end, RangeWriter writer, CancellationToken cancellationToken);
}
