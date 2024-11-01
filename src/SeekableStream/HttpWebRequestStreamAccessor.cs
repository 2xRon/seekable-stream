using System.Threading.Tasks;
using System.Threading;
using System;
using System.Net;
using System.Net.Http.Headers;

namespace SeekableStream;

#pragma warning disable SYSLIB0014 // Type or member is obsolete
public class HttpWebRequestStreamAccessor : IRangeStreamAccessor
{
    private readonly Uri _requestUri;
    private readonly Action<HttpWebRequest>? _configureRangeRequest;
    private readonly Action<HttpWebRequest>? _configureLengthRequest;

    public HttpWebRequestStreamAccessor(Uri requestUri, Action<HttpWebRequest>? configureRangeRequest = null, Action<HttpWebRequest>? configureLengthRequest = null)
    {
        _requestUri = requestUri;
        _configureRangeRequest = configureRangeRequest;
        _configureLengthRequest = configureLengthRequest;
    }

    protected virtual long GetLength()
    {
        try
        {
            var request = (HttpWebRequest)WebRequest.Create(_requestUri);
            request.Method = "HEAD";
            request.AddRange(0, 0);
            _configureLengthRequest?.Invoke(request);

            using var response = (HttpWebResponse)request.GetResponse();
            if (response.StatusCode != HttpStatusCode.PartialContent)
            {
                throw new InvalidOperationException("Invalid length query response");
            }

            var contentRangeHeader = response.Headers["Content-Range"];
            if (
                !string.IsNullOrEmpty(contentRangeHeader)
                && ContentRangeHeaderValue.TryParse(contentRangeHeader, out var value)
                && value.HasLength
                && value.Length.HasValue)
            {
                return value.Length.Value;
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

    private long? _length;
    public virtual long Length => _length ??= GetLength();

    public int ReadRange(long start, long end, RangeWriter writer, CancellationToken cancellationToken)
    {
        var request = CreateRequest(start, end);
        using var response = request.GetResponse();
        using var stream = response.GetResponseStream();
        return writer(stream);
    }

    public async Task<int> ReadRangeAsync(long start, long end, RangeWriterAsync writer, CancellationToken cancellationToken)
    {
        var request = CreateRequest(start, end);
        using var response = await request.GetResponseAsync();
        using var stream = response.GetResponseStream();
        return await writer(stream, cancellationToken);
    }

    protected virtual HttpWebRequest CreateRequest(long start, long end)
    {
        var request = (HttpWebRequest)WebRequest.Create(_requestUri);
        request.Method = "GET";
        request.AddRange(start, end);
        _configureRangeRequest?.Invoke(request);
        return request;
    }
}
#pragma warning restore SYSLIB0014 // Type or member is obsolete
