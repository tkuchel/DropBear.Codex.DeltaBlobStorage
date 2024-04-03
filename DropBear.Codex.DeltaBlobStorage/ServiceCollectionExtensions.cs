using DropBear.Codex.DeltaBlobStorage.Services;
using DropBear.Codex.Files;
using Microsoft.Extensions.DependencyInjection;

namespace DropBear.Codex.DeltaBlobStorage;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddDeltaBlobStorage(this IServiceCollection services)
    {
        services.AddDropBearCodexFiles();
        services.AddScoped<ExtendedFileManager>();

        return services;
    }
}