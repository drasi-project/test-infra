using Aspire.Hosting.ApplicationModel;
using Azure.Provisioning.AppContainers;

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
    .WithArgs("--config", "/app/config/drasi-server.empty.yaml")
    .WithHttpEndpoint(targetPort: 8080, name: "http")
    .WithEndpoint(targetPort: 50051, name: "grpc-source")
    .WithHttpHealthCheck("/health");

if (builder.ExecutionContext.IsRunMode)
{
    drasiServer.WithBindMount(drasiServerConfigDirectory, "/app/config");
}

var redis = builder.AddRedis("redis");

var e2eRunner = builder.AddDockerfile(
        "e2e-runner",
        repoRoot,
        "aspire/Drasi.E2E.Runner/Dockerfile")
    .WithBindMount(artifactsDirectory, "/artifacts")
    .WithEndpoint(targetPort: 63123, name: "test-service")
    .WithEndpoint(targetPort: 50052, name: "reaction-main")
    .WithEndpoint(targetPort: 50053, name: "reaction-floor-agg")
    .WithEnvironment("DRASI_SERVER_URL", drasiServer.GetEndpoint("http"))
    .WithEnvironment("DRASI_SOURCE_ENDPOINT", drasiServer.GetEndpoint("grpc-source"))
    .WithEnvironment("E2E_REACTION_HOST", "e2e-runner")
    .WithEnvironment("TEST_SERVICE_PORT", "63123")
    .WithEnvironment("ARTIFACTS_DIR", "/artifacts")
    .WithEnvironment(
        "KEEP_ALIVE_AFTER_COMPLETION",
        builder.ExecutionContext.IsPublishMode ? "true" : "false")
    .WithReference(redis)
    .WaitFor(drasiServer)
    .WaitFor(redis);

WithOptionalEnvironment(e2eRunner, "TIMEOUT_SECS");
WithOptionalEnvironment(e2eRunner, "POLL_INTERVAL_SECS");
WithOptionalEnvironment(e2eRunner, "TEST_RUN_ID");

if (builder.ExecutionContext.IsPublishMode)
{
    var workloadProfile = Environment.GetEnvironmentVariable("AZURE_WORKLOAD_PROFILE")?
        .Trim()
        .ToLowerInvariant() ?? "consumption";

    switch (workloadProfile)
    {
        case "consumption":
            builder.AddAzureContainerAppEnvironment("drasi-e2e-aca");
            ConfigureAzureContainerApp(drasiServer, "consumption", cpu: 4.0, memory: "8Gi");
            ConfigureAzureContainerApp(e2eRunner, "consumption", cpu: 4.0, memory: "8Gi");
            ConfigureAzureContainerApp(redis, "consumption", cpu: 0.5, memory: "1Gi");
            break;

        case "dedicated-d8":
            const string dedicatedProfileName = "dedicated-d8";
            builder.AddAzureContainerAppEnvironment("drasi-e2e-aca")
                .ConfigureInfrastructure(infrastructure =>
                {
                    var environment = infrastructure.GetProvisionableResources()
                        .OfType<ContainerAppManagedEnvironment>()
                        .Single();

                    environment.WorkloadProfiles.Add(new ContainerAppWorkloadProfile
                    {
                        Name = dedicatedProfileName,
                        WorkloadProfileType = "D8",
                        MinimumNodeCount = 1,
                        MaximumNodeCount = 1
                    });
                });

            ConfigureAzureContainerApp(drasiServer, dedicatedProfileName, cpu: 4.0, memory: "16Gi");
            ConfigureAzureContainerApp(e2eRunner, dedicatedProfileName, cpu: 3.0, memory: "12Gi");
            ConfigureAzureContainerApp(redis, dedicatedProfileName, cpu: 0.5, memory: "2Gi");
            break;

        default:
            throw new InvalidOperationException(
                $"Unsupported AZURE_WORKLOAD_PROFILE '{workloadProfile}'. Use 'consumption' or 'dedicated-d8'.");
    }
}

builder.Build().Run();

static void ConfigureAzureContainerApp<T>(
    IResourceBuilder<T> resource,
    string workloadProfileName,
    double cpu,
    string memory)
    where T : ContainerResource
{
    resource.PublishAsAzureContainerApp((_, app) =>
    {
        app.WorkloadProfileName = workloadProfileName;
        app.Template.Containers[0].Value!.Resources.Cpu = cpu;
        app.Template.Containers[0].Value!.Resources.Memory = memory;
        app.Template.Scale.MinReplicas = 1;
        app.Template.Scale.MaxReplicas = 1;
    });
}

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
