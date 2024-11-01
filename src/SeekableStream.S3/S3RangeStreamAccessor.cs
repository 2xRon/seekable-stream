using Amazon.S3;
using Amazon.S3.Model;
using System.Threading;
using System.Threading.Tasks;

namespace SeekableStream.S3
{
    public class S3RangeStreamAccessor : IRangeStreamAccessor
    {
        private readonly IAmazonS3 _s3;
        private readonly string _bucket;
        private readonly string _key;
        private readonly string _s3ETag;

        public S3RangeStreamAccessor(IAmazonS3 s3, string bucket, string key)
        {
            _s3 = s3;
            _bucket = bucket;
            _key = key;
            var m =  s3.GetObjectMetadataAsync(bucket, key).GetAwaiter().GetResult();
            Length = m.ContentLength;
            _s3ETag = m.ETag;
        }

        public long Length { get; }

        public int ReadRange(long start, long end, RangeWriter writer, CancellationToken cancellationToken)
        {
            var go = new GetObjectRequest()
            {
                BucketName = _bucket,
                Key = _key,
                EtagToMatch = _s3ETag, // ensure the object hasn't changed under us
                ByteRange = new ByteRange(start, end)
            };
            using var response = _s3.GetObjectAsync(go, cancellationToken).GetAwaiter().GetResult(); // :(
            return writer(response.ResponseStream);
        }

        public async Task<int> ReadRangeAsync(long start, long end, RangeWriterAsync writer, CancellationToken cancellationToken)
        {
            var go = new GetObjectRequest()
            {
                BucketName = _bucket,
                Key = _key,
                EtagToMatch = _s3ETag, // ensure the object hasn't changed under us
                ByteRange = new ByteRange(start, end)
            };
            using var response = await _s3.GetObjectAsync(go, cancellationToken);
            return await writer(response.ResponseStream, cancellationToken);
        }
    }
}
