using Azure;
using Azure.Monitor.OpenTelemetry.AspNetCore;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using OpenTelemetry;
using OpenTelemetry.Trace;
using System.Diagnostics;

var builder = WebApplication.CreateBuilder(args);

// Register BlobServiceClient as singleton
var storageConnectionString = builder.Configuration["AzureStorage:ConnectionString"]
    ?? throw new InvalidOperationException("AzureStorage:ConnectionString not configured");
builder.Services.AddSingleton(new BlobServiceClient(storageConnectionString));

// Register LeaseStateManager for tracking lease state
builder.Services.AddSingleton<LeaseStateManager>();

// Configure OpenTelemetry with Application Insights
builder.Services.AddOpenTelemetry()
    .UseAzureMonitor(options =>
    {
        options.ConnectionString = builder.Configuration["ApplicationInsights:ConnectionString"]
            ?? throw new InvalidOperationException("ApplicationInsights:ConnectionString not configured");
    })
    .WithTracing(tracing =>
    {
        tracing.AddAspNetCoreInstrumentation();
        tracing.AddHttpClientInstrumentation();
        tracing.AddProcessor<LeaseErrorFilterProcessor>();
    });

var app = builder.Build();

// PUT /lock endpoint - Acquire infinite lease on blob
app.MapPut("/lock", async (
    BlobServiceClient blobClient,
    LeaseStateManager state,
    IConfiguration config,
    ILogger<Program> logger) =>
{
    var containerName = config["AzureStorage:ContainerName"]!;
    var blobName = config["AzureStorage:BlobName"]!;

    logger.LogInformation("Attempting to acquire lease on blob: {ContainerName}/{BlobName}", containerName, blobName);

    try
    {
        var containerClient = blobClient.GetBlobContainerClient(containerName);
        var blob = containerClient.GetBlobClient(blobName);
        var leaseClient = blob.GetBlobLeaseClient();

        // Acquire infinite lease (-1 duration)
        var lease = await leaseClient.AcquireAsync(TimeSpan.FromSeconds(-1));
        var leaseId = lease.Value.LeaseId;

        state.SetLeaseId(leaseId);
        logger.LogInformation("Lease acquired successfully: {LeaseId}", leaseId);

        return Results.Ok(new { leaseId });
    }
    catch (RequestFailedException ex)
    {
        logger.LogError(ex, "Failed to acquire lease: {ErrorCode} - {Message}", ex.ErrorCode, ex.Message);
        return Results.Problem(
            detail: ex.Message,
            statusCode: ex.Status,
            title: $"Azure Storage Error: {ex.ErrorCode}");
    }
});

// PUT /unlock endpoint - Release the lease
app.MapPut("/unlock", async (
    BlobServiceClient blobClient,
    LeaseStateManager state,
    IConfiguration config,
    ILogger<Program> logger) =>
{
    var leaseId = state.GetLeaseId();
    if (leaseId == null)
    {
        logger.LogWarning("Unlock attempted but no active lease found");
        return Results.BadRequest(new { error = "No active lease to release" });
    }

    var containerName = config["AzureStorage:ContainerName"]!;
    var blobName = config["AzureStorage:BlobName"]!;

    logger.LogInformation("Attempting to release lease: {LeaseId}", leaseId);

    try
    {
        var containerClient = blobClient.GetBlobContainerClient(containerName);
        var blob = containerClient.GetBlobClient(blobName);
        var leaseClient = blob.GetBlobLeaseClient(leaseId);

        await leaseClient.ReleaseAsync();
        state.ClearLeaseId();

        logger.LogInformation("Lease released successfully: {LeaseId}", leaseId);
        return Results.Ok(new { message = "Lease released successfully", leaseId });
    }
    catch (RequestFailedException ex)
    {
        logger.LogError(ex, "Failed to release lease: {ErrorCode} - {Message}", ex.ErrorCode, ex.Message);
        return Results.Problem(
            detail: ex.Message,
            statusCode: ex.Status,
            title: $"Azure Storage Error: {ex.ErrorCode}");
    }
});

app.Run();

// Helper class for managing lease state across requests
class LeaseStateManager
{
    private string? _currentLeaseId;
    private readonly object _lock = new();

    public void SetLeaseId(string leaseId)
    {
        lock (_lock)
        {
            _currentLeaseId = leaseId;
        }
    }

    public string? GetLeaseId()
    {
        lock (_lock)
        {
            return _currentLeaseId;
        }
    }

    public void ClearLeaseId()
    {
        lock (_lock)
        {
            _currentLeaseId = null;
        }
    }
}

// Custom OpenTelemetry processor to filter Azure Blob Storage lease errors
class LeaseErrorFilterProcessor : BaseProcessor<Activity>
{
    private readonly ILogger<LeaseErrorFilterProcessor> _logger;

    public LeaseErrorFilterProcessor(ILogger<LeaseErrorFilterProcessor> logger)
    {
        _logger = logger;
    }

    public override void OnEnd(Activity activity)
    {
        // Check if this is an Azure Storage activity
        if (activity.Source.Name.StartsWith("Azure.Storage", StringComparison.OrdinalIgnoreCase))
        {
            // Inspect activity tags for error information
            var errorCode = activity.GetTagItem("error.code")?.ToString();
            var errorType = activity.GetTagItem("error.type")?.ToString();
            var exceptionType = activity.GetTagItem("exception.type")?.ToString();
            var httpStatusCode = activity.GetTagItem("http.status_code")?.ToString();

            // Check if this is a lease-related error
            var isLeaseError = IsLeaseRelatedError(errorCode, errorType, exceptionType, httpStatusCode);

            if (isLeaseError)
            {
                _logger.LogDebug(
                    "Filtering lease error activity - Name: {ActivityName}, ErrorCode: {ErrorCode}, ErrorType: {ErrorType}, StatusCode: {StatusCode}",
                    activity.DisplayName,
                    errorCode,
                    errorType,
                    httpStatusCode);

                // Suppress this activity by not calling base.OnEnd
                // This prevents it from being exported to Application Insights
                return;
            }
        }

        // Allow all other activities through
        base.OnEnd(activity);
    }

    private bool IsLeaseRelatedError(string? errorCode, string? errorType, string? exceptionType, string? httpStatusCode)
    {
        // Check for known lease error codes
        if (errorCode != null && (
            errorCode.Contains("Lease", StringComparison.OrdinalIgnoreCase) ||
            errorCode.Equals("LeaseAlreadyPresent", StringComparison.OrdinalIgnoreCase) ||
            errorCode.Equals("LeaseIdMismatchWithLeaseOperation", StringComparison.OrdinalIgnoreCase) ||
            errorCode.Equals("LeaseNotPresentWithLeaseOperation", StringComparison.OrdinalIgnoreCase) ||
            errorCode.Equals("LeaseIdMissing", StringComparison.OrdinalIgnoreCase)))
        {
            return true;
        }

        // Check error type
        if (errorType != null && errorType.Contains("Lease", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // Check exception type
        if (exceptionType != null && exceptionType.Contains("Lease", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        // 409 status code with Azure Storage often indicates lease conflict
        // Note: This is a heuristic - may need refinement based on actual testing
        if (httpStatusCode == "409")
        {
            return true;
        }

        return false;
    }
}
