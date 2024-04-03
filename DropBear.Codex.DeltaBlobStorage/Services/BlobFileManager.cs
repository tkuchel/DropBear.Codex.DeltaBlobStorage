using DropBear.Codex.AppLogger.Interfaces;
using DropBear.Codex.Core.ReturnTypes;
using DropBear.Codex.Files.Services;
using DropBear.Codex.Utilities.MessageTemplates;
using DropBear.Codex.Validation.StrategyValidation.Interfaces;
using FluentStorage;
using FluentStorage.Blobs;

namespace DropBear.Codex.DeltaBlobStorage.Services;

public class BlobFileManager : FileManager
{
    private readonly IAppLogger<BlobFileManager> _logger;
    private IBlobStorage? _blobStorage;

    public BlobFileManager(IAppLogger<FileManager> baseLogger, IStrategyValidator strategyValidator,
        IMessageTemplateManager messageTemplateManager)
        : base(baseLogger, strategyValidator, messageTemplateManager) =>
        _logger = baseLogger.ForContext<BlobFileManager>();

    public string ContainerName { get; set; } = "default";
    public string? BlobAccountName { get; set; }
    public string? BlobSharedKey { get; set; }

    private Result BuildBlobStorageFromNameAndKey()
    {
        try
        {
            if (_blobStorage is not null) return Result.Success();

            if (string.IsNullOrWhiteSpace(BlobAccountName))
            {
                _logger.LogError("Blob account name or shared key is null or empty.");
                throw new ArgumentNullException(nameof(BlobAccountName));
            }

            if (string.IsNullOrWhiteSpace(BlobSharedKey))
            {
                _logger.LogError("Blob shared key is null or empty.");
                throw new ArgumentNullException(nameof(BlobSharedKey));
            }

            var blobStorage = StorageFactory.Blobs.AzureBlobStorageWithSharedKey(BlobAccountName, BlobSharedKey).WithGzipCompression();
            InitializeBlobStorage(blobStorage);
            return Result.Success();
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to build blob storage from account name and shared key.");
            return Result.Failure("FailedToBuildBlobStorageFromNameAndKey");
        }
    }
    private void InitializeBlobStorage(IBlobStorage blobStorage) =>
        _blobStorage = blobStorage ?? throw new ArgumentNullException(nameof(blobStorage));

    public async Task<Result<byte[]>> ReadBlobByIdAsync(string blobId)
    {
        if (_blobStorage is null)
        {
            _logger.LogError("Blob storage has not been initialized.");
            return Result<byte[]>.Failure("BlobStorageNotInitialized");
        }

        var blobPath = NormalizeFilePath(blobId);
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
    private string NormalizeFilePath(string blobId)
    {
        if (string.IsNullOrWhiteSpace(blobId))
        {
            _logger.LogError("Blob ID is null or empty.");
            throw new ArgumentNullException(nameof(blobId));
        }

        // Normalize the blob ID to ensure it is a valid file path.
        if (!blobId.Contains('/', StringComparison.OrdinalIgnoreCase))
            // If the blob ID does not contain any path separators, prepend default container name and /
            blobId = $"{ContainerName}/{blobId}";
        var blobPath = blobId.Replace('/', Path.DirectorySeparatorChar);
        return blobPath;
    }

    public async Task<Result> WriteBlobByIdAsync(string blobId, byte[] data)
    {
        if (_blobStorage is null)
        {
            _logger.LogError("Blob storage has not been initialized.");
            return Result.Failure("BlobStorageNotInitialized");
        }

        var blobPath = NormalizeFilePath(blobId);

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
}
