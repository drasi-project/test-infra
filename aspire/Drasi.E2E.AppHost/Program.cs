using Aspire.Hosting.ApplicationModel;

var builder = DistributedApplication.CreateBuilder(args);

var repoRoot = FindRepoRoot();
var appHostDirectory = Path.Combine(repoRoot, "aspire", "Drasi.E2E.AppHost");
var artifactsDirectory = Path.Combine(appHostDirectory, "artifacts");
var drasiServerConfigDirectory = Path.Combine(appHostDirectory, "config");

Directory.CreateDirectory(artifactsDirectory);

var drasiServerImage = Environment.GetEnvironmentVariable("DRASI_SERVER_IMAGE");

var drasiServer = string.IsNullOrWhiteSpace(drasiServerImage)
    ? builder.AddDockerfile("drasi-server", repoRoot, "aspire/Drasi.Server.Container/Dockerfile")
    : builder.AddContainer("drasi-server", drasiServerImage);

drasiServer
    .WithBindMount(drasiServerConfigDirectory, "/app/config")
    .WithArgs("--config", "/app/config/drasi-server.empty.yaml")
    .WithHttpEndpoint(targetPort: 8080, name: "http")
    .WithEndpoint(targetPort: 50051, name: "grpc-source")
    .WithHttpHealthCheck("/health");

var redis = builder.AddRedis("redis");

var e2eRunner = builder.AddDockerfile(
        "e2e-runner",
        repoRoot,
        "aspire/Drasi.E2E.Runner/Dockerfile")
    .WithBindMount(artifactsDirectory, "/artifacts")
    .WithEndpoint(targetPort: 63123, name: "test-service")
    .WithEndpoint(targetPort: 50052, name: "reaction-main")
    .WithEndpoint(targetPort: 50053, name: "reaction-floor-agg")
    .WithEnvironment("DRASI_SERVER_URL", "http://drasi-server:8080")
    .WithEnvironment("DRASI_SOURCE_HOST", "drasi-server")
    .WithEnvironment("DRASI_SOURCE_PORT", "50051")
    .WithEnvironment("E2E_REACTION_HOST", "e2e-runner")
    .WithEnvironment("TEST_SERVICE_PORT", "63123")
    .WithEnvironment("REDIS_CONNECTION_STRING", "redis://redis:6379")
    .WithEnvironment("ARTIFACTS_DIR", "/artifacts")
    .WithReference(redis)
    .WaitFor(drasiServer)
    .WaitFor(redis);

WithOptionalEnvironment(e2eRunner, "TIMEOUT_SECS");
WithOptionalEnvironment(e2eRunner, "POLL_INTERVAL_SECS");
WithOptionalEnvironment(e2eRunner, "TEST_RUN_ID");

builder.Build().Run();

static void WithOptionalEnvironment(
    IResourceBuilder<ContainerResource> resource,
    string environmentVariableName)
{
    var value = Environment.GetEnvironmentVariable(environmentVariableName);
    if (!string.IsNullOrWhiteSpace(value))
    {
        resource.WithEnvironment(environmentVariableName, value);
    }
}

static string FindRepoRoot()
{
    foreach (var start in new[] { Directory.GetCurrentDirectory(), AppContext.BaseDirectory })
    {
        var directory = new DirectoryInfo(start);
        while (directory is not null)
        {
            if (File.Exists(Path.Combine(directory.FullName, "e2e-test-framework", "Cargo.toml")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }
    }

    throw new InvalidOperationException(
        "Could not locate the repository root. Run the AppHost from inside the test-infra checkout.");
}
