using DropBear.Codex.AppLogger.Interfaces;
using DropBear.Codex.Files.Services;
using DropBear.Codex.Utilities.MessageTemplates;
using DropBear.Codex.Validation.StrategyValidation.Interfaces;

namespace DropBear.Codex.DeltaBlobStorage.Services;

public class ExtendedFileManager : FileManager
{
    
    private readonly IAppLogger<BlobFileManager> _logger;

    public ExtendedFileManager(IAppLogger<FileManager> baseLogger, IStrategyValidator strategyValidator,
        IMessageTemplateManager messageTemplateManager)
        : base(baseLogger, strategyValidator, messageTemplateManager) =>
        _logger = baseLogger.ForContext<BlobFileManager>();
    
    

}
