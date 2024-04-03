using DropBear.Codex.AppLogger.Interfaces;
using DropBear.Codex.Core.ReturnTypes;
using DropBear.Codex.Files.Services;
using DropBear.Codex.Utilities.MessageTemplates;
using DropBear.Codex.Validation.StrategyValidation.Interfaces;
using FastRsync.Delta;
using FastRsync.Diagnostics;
using FastRsync.Signature;
using FluentStorage.Blobs;

namespace DropBear.Codex.DeltaBlobStorage.Services;

public class ExtendedFileManager : FileManager
{
    private const string DefaultContainerName = "default/";
    private readonly IAppLogger<ExtendedFileManager> _logger;
    private IBlobStorage? _blobStorage;

    public ExtendedFileManager(IAppLogger<FileManager> baseLogger, IStrategyValidator strategyValidator,
        IMessageTemplateManager messageTemplateManager)
        : base(baseLogger, strategyValidator, messageTemplateManager) =>
        _logger = baseLogger.ForContext<ExtendedFileManager>();

    public void InitializeBlobStorage(IBlobStorage blobStorage) =>
        _blobStorage = blobStorage ?? throw new ArgumentNullException(nameof(blobStorage));

    public async Task<Result<byte[]>> CalculateFileSignatureAsync(string basisFileId)
    {
        if (_blobStorage is null)
        {
            _logger.LogError("Blob storage must be initialized before calculating the file signature.");
            return Result<byte[]>.Failure("BlobStorageNotInitialized");
        }

        var basisFileResult = await GetBlobByIdAsync(basisFileId).ConfigureAwait(false);

        if (basisFileResult.IsFailure)
        {
            _logger.LogError($"Failed to find the basis file: {basisFileId}");
            return Result<byte[]>.Failure(basisFileResult.ErrorMessage);
        }

        var signatureBuilder = new SignatureBuilder();
        var basisFileStream = new MemoryStream(basisFileResult.Value);
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

    public async Task<Result<byte[]>> CalculateDeltaBetweenBasisFileAndNewFileAsync(byte[]? signatureFileData,
        byte[]? newFileData)
    {
        if (signatureFileData is null || signatureFileData.Length is 0)
        {
            _logger.LogError("Signature file data is empty.");
            return Result<byte[]>.Failure("EmptySignatureFileData");
        }

        if (newFileData is null || newFileData.Length is 0)
        {
            _logger.LogError("New file data is empty.");
            return Result<byte[]>.Failure("EmptyNewFileData");
        }

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
            _logger.LogError(e, "Failed to calculate delta between basis file and new file.");
            return Result<byte[]>.Failure("FailedToCalculateDeltaBetweenBasisFileAndNewFile");
        }
    }


    public async Task<Result<byte[]>> ApplyDeltaToBasisFileAsync(byte[]? basisFileData, byte[]? deltaFileData)
    {
        if (basisFileData is null || basisFileData.Length is 0)
        {
            _logger.LogError("Basis file data is empty.");
            return Result<byte[]>.Failure("EmptyBasisFileData");
        }

        if (deltaFileData is null || deltaFileData.Length is 0)
        {
            _logger.LogError("Delta file data is empty.");
            return Result<byte[]>.Failure("EmptyDeltaFileData");
        }

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
            _logger.LogError(e, "Failed to apply delta to basis file.");
            return Result<byte[]>.Failure("FailedToApplyDeltaToBasisFile");
        }
    }


    private async Task<Result<byte[]>> GetBlobByIdAsync(string blobId)
    {
        if (_blobStorage is null)
        {
            _logger.LogError("Blob storage has not been initialized.");
            return Result<byte[]>.Failure("BlobStorageNotInitialized");
        }

        var blobPath = NormalizeBasisFilePath(blobId);
        var blobExists = await _blobStorage.ExistsAsync(new[] { blobPath }).ConfigureAwait(false);

        if (!blobExists.FirstOrDefault())
        {
            _logger.LogError($"Blob not found: {blobPath}");
            return Result<byte[]>.Failure("BlobNotFound");
        }

        var memoryStream = new MemoryStream();
        Stream? blobStream = null; // Declare outside to ensure visibility in the finally block

        try
        {
            blobStream = await _blobStorage.OpenReadAsync(blobPath).ConfigureAwait(false);

            if (blobStream is null)
            {
                _logger.LogError($"Failed to open blob for reading: {blobPath}");
                return Result<byte[]>.Failure("BlobOpenReadFailed");
            }

            await blobStream.CopyToAsync(memoryStream).ConfigureAwait(false);
        }
        finally
        {
            if (blobStream is not null) await blobStream.DisposeAsync().ConfigureAwait(false);
        }

        memoryStream.Position = 0; // Reset the position to the beginning after copying.
        return Result<byte[]>.Success(memoryStream.ToArray());
    }

    public async Task<Result> WriteBlobByIdAsync(string blobId, byte[] data)
    {
        if (_blobStorage is null)
        {
            _logger.LogError("Blob storage has not been initialized.");
            return Result.Failure("BlobStorageNotInitialized");
        }

        var blobPath = NormalizeBasisFilePath(blobId);

        // Check if the blob already exists to prevent unintentional overwrites.
        var blobExists = await _blobStorage.ExistsAsync(new[] { blobPath }).ConfigureAwait(false);
        if (blobExists.FirstOrDefault())
        {
            _logger.LogError(
                $"Attempted to write to an existing blob: {blobPath}. Consider using an update method instead.");
            return Result.Failure("BlobAlreadyExists");
        }

        try
        {
            var dataStream = new MemoryStream(data);
            await _blobStorage.WriteAsync(blobPath, dataStream).ConfigureAwait(false);
            return Result.Success();
        }
        catch (Exception e)
        {
            _logger.LogError(e, $"Failed to write blob by id: {blobId}.");
            return Result.Failure("FailedToWriteBlobById");
        }
    }


    private static string NormalizeBasisFilePath(string basisFilePath) =>
        // Ensure the path has a container prefix if missing
        basisFilePath.Contains('/', StringComparison.OrdinalIgnoreCase)
            ? basisFilePath
            : DefaultContainerName + basisFilePath;
}
