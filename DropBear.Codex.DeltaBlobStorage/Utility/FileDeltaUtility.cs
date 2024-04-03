using DropBear.Codex.Core.ReturnTypes;
using FastRsync.Delta;
using FastRsync.Diagnostics;
using FastRsync.Signature;

namespace DropBear.Codex.DeltaBlobStorage.Utility;

public static class FileDeltaUtility
{
    public static async Task<Result<byte[]>> CalculateFileSignatureAsync(byte[]? basisFileData)
    {
        if (basisFileData is null || basisFileData.Length is 0) return Result<byte[]>.Failure("EmptyBasisFileData");

        var signatureBuilder = new SignatureBuilder();
        var basisFileStream = new MemoryStream(basisFileData);
        var signatureStream = new MemoryStream();

        try
        {
            await signatureBuilder.BuildAsync(basisFileStream, new SignatureWriter(signatureStream))
                .ConfigureAwait(false);
            return Result<byte[]>.Success(signatureStream.ToArray());
        }
        finally
        {
            // Explicitly await the disposal of streams with ConfigureAwait(false)
            await basisFileStream.DisposeAsync().ConfigureAwait(false);
            await signatureStream.DisposeAsync().ConfigureAwait(false);
        }
    }

    public static async Task<Result<byte[]>> CalculateDeltaBetweenBasisFileAndNewFileAsync(byte[]? signatureFileData,
        byte[]? newFileData)
    {
        if (signatureFileData is null || signatureFileData.Length is 0)
            return Result<byte[]>.Failure("EmptySignatureFileData");

        if (newFileData is null || newFileData.Length is 0) return Result<byte[]>.Failure("EmptyNewFileData");

        try
        {
            var progressReporter = new ConsoleProgressReporter();
            var deltaBuilder = new DeltaBuilder();
            var deltaStream = new MemoryStream();
            var newFileStream = new MemoryStream(newFileData);
            var signatureStream = new MemoryStream(signatureFileData);
            try
            {
                var signatureReader = new SignatureReader(signatureStream, progressReporter);
                var deltaWriter = new BinaryDeltaWriter(deltaStream);

                await deltaBuilder.BuildDeltaAsync(newFileStream, signatureReader, deltaWriter).ConfigureAwait(false);
                return Result<byte[]>.Success(deltaStream.ToArray());
            }
            finally
            {
                // Manually dispose of the streams to ensure resources are freed up properly.
                await deltaStream.DisposeAsync().ConfigureAwait(false);
                await newFileStream.DisposeAsync().ConfigureAwait(false);
                await signatureStream.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            return Result<byte[]>.Failure("FailedToCalculateDeltaBetweenBasisFileAndNewFile");
        }
    }

    public static async Task<Result<byte[]>> ApplyDeltaToBasisFileAsync(byte[]? basisFileData, byte[]? deltaFileData)
    {
        if (basisFileData is null || basisFileData.Length is 0) return Result<byte[]>.Failure("EmptyBasisFileData");

        if (deltaFileData is null || deltaFileData.Length is 0) return Result<byte[]>.Failure("EmptyDeltaFileData");

        try
        {
            var progressReporter = new ConsoleProgressReporter();
            var deltaApplier = new DeltaApplier { SkipHashCheck = true };
            var deltaStream = new MemoryStream(deltaFileData);
            var basisFileStream = new MemoryStream(basisFileData);
            var resultStream = new MemoryStream();

            try
            {
                await deltaApplier
                    .ApplyAsync(basisFileStream, new BinaryDeltaReader(deltaStream, progressReporter), resultStream)
                    .ConfigureAwait(false);

                return Result<byte[]>.Success(resultStream.ToArray());
            }
            finally
            {
                // Ensure resources are cleaned up correctly
                await deltaStream.DisposeAsync().ConfigureAwait(false);
                await basisFileStream.DisposeAsync().ConfigureAwait(false);
                await resultStream.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            return Result<byte[]>.Failure("FailedToApplyDeltaToBasisFile");
        }
    }
}
