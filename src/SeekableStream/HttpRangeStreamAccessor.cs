using System.Net.Http;
using System.Threading.Tasks;
using System.Threading;
using System;
using System.Net;
using System.Net.Http.Headers;
using System.IO;

namespace SeekableStream;

public class HttpRangeStreamAccessor : IRangeStreamAccessor
{
    private readonly HttpClient _httpClient;
    private readonly Uri _fileUri;

    public HttpRangeStreamAccessor(HttpClient httpClient, Uri fileUri)
    {
        _httpClient = httpClient;
        _fileUri = fileUri;
    }

    protected virtual long GetLength()
    {
        try
        {
            using var headRequest = new HttpRequestMessage(HttpMethod.Head, _fileUri);
            headRequest.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(0, 0);
#if NET5_0_OR_GREATER
            using var headResponse = _httpClient.Send(headRequest);
#else
            using var headResponse = _httpClient.SendAsync(headRequest).GetAwaiter().GetResult();
#endif
            headResponse.EnsureSuccessStatusCode();
            if (headResponse.Content.Headers.ContentRange?.HasLength == true)
            {
                return headResponse.Content.Headers.ContentRange.Length!.Value;
            }
            else
            {
                throw new InvalidOperationException("Cannot determine file length from server.");
            }
        }
        catch (Exception ex)
        {
            throw new InvalidOperationException("Cannot determine file length from server.", ex);
        }
    }

    protected long? _length;
    public long Length => _length ??= GetLength();

    public int ReadRange(long start, long end, RangeWriter writer, CancellationToken cancellationToken)
    {
        var request = CreateRequest(start, end);
#if NET5_0_OR_GREATER
        using var response = _httpClient.Send(request, cancellationToken);
        response.EnsureSuccessStatusCode();
        using var content = response.Content;
        using var stream = content.ReadAsStream(cancellationToken);
        return writer(stream);
#else
        using var response = _httpClient.SendAsync(request, cancellationToken).GetAwaiter().GetResult();
        response.EnsureSuccessStatusCode();
        using var content = response.Content;
        using var stream = content.ReadAsStreamAsync().GetAwaiter().GetResult();
        return writer(stream);
#endif
    }

    public async Task<int> ReadRangeAsync(long start, long end, RangeWriterAsync writer, CancellationToken cancellationToken)
    {
        var request = CreateRequest(start, end);
        using var response = _httpClient.SendAsync(request, cancellationToken).GetAwaiter().GetResult();
        response.EnsureSuccessStatusCode();
        using var content = response.Content;
#if NET5_0_OR_GREATER
        using var stream = await content.ReadAsStreamAsync(cancellationToken);
#else
        using var stream = await content.ReadAsStreamAsync();
#endif
        return await writer(stream, cancellationToken);
    }

    protected virtual HttpRequestMessage CreateRequest(long start, long end)
    {
        using var request = new HttpRequestMessage(HttpMethod.Get, _fileUri);
        request.Headers.Range = new System.Net.Http.Headers.RangeHeaderValue(start, end);
        return request;
    }
}
